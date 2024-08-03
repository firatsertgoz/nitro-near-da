// Copyright 2021-2022, Offchain Labs, Inc.
// For license information, see https://github.com/nitro/blob/master/LICENSE

package das

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbutil"
	"github.com/offchainlabs/nitro/solgen/go/bridgegen"
	"github.com/offchainlabs/nitro/util/headerreader"
	"github.com/offchainlabs/nitro/util/signature"
)

// CreatePersistentStorageService creates any storage services that persist to files, database, cloud storage,
// and group them together into a RedundantStorage instance if there is more than one.
func CreatePersistentStorageService(
	ctx context.Context,
	config *DataAvailabilityConfig,
) (StorageService, *LifecycleManager, error) {
	storageServices := make([]StorageService, 0, 10)
	var lifecycleManager LifecycleManager
	var err error

	var fs *LocalFileStorageService
	if config.LocalFileStorage.Enable {
		fs, err = NewLocalFileStorageService(config.LocalFileStorage)
		if err != nil {
			return nil, nil, err
		}
		err = fs.start(ctx)
		if err != nil {
			return nil, nil, err
		}
		lifecycleManager.Register(fs)
		storageServices = append(storageServices, fs)
	}

	if config.LocalDBStorage.Enable {
		var s *DBStorageService
		if config.MigrateLocalDBToFileStorage {
			s, err = NewDBStorageService(ctx, &config.LocalDBStorage, fs)
		} else {
			s, err = NewDBStorageService(ctx, &config.LocalDBStorage, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		if s != nil {
			lifecycleManager.Register(s)
			storageServices = append(storageServices, s)
		}
	}

	if config.S3Storage.Enable {
		s, err := NewS3StorageService(config.S3Storage)
		if err != nil {
			return nil, nil, err
		}
		lifecycleManager.Register(s)
		storageServices = append(storageServices, s)
	}

	if len(storageServices) > 1 {
		s, err := NewRedundantStorageService(ctx, storageServices)
		if err != nil {
			return nil, nil, err
		}
		lifecycleManager.Register(s)
		return s, &lifecycleManager, nil
	}
	if len(storageServices) == 1 {
		return storageServices[0], &lifecycleManager, nil
	}
	if len(storageServices) == 0 {
		return nil, nil, errors.New("No data-availability storage backend has been configured")
	}

	return nil, &lifecycleManager, nil
}

func CreateNearDAS(
	ctx context.Context,
	config *DataAvailabilityConfig,
	lifecycleManager *LifecycleManager,
) (DataAvailabilityServiceWriter, DataAvailabilityServiceReader, *NearServiceSidecar, error) {
	if !config.NEARAggregator.Enable {
		return nil, nil, nil, errors.New("--node.data-availability.near-aggregator.enable must be set")
	}

	log.Info("Initialising near service")
	nearSvc, err := NewNearServiceSidecar(*config)
	if err != nil {
		log.Error("initialising near service", "error", err)
		return nil, nil, nil, err
	}
	var nearWriter DataAvailabilityServiceWriter = nearSvc
	// log.Info("initialising near aggregator")
	//nearAggr, err = NewNearAggregator(ctx, *config, nearSvc)
	// if err != nil {
	// 	return nil, nil, err
	// }

	var nearReader DataAvailabilityServiceReader = nearSvc
	log.Info("initialising Chain Fetch Reader")
	// FIXME: lifecycleManager.Register(nearr)
	if err != nil {
		return nil, nil, nil, err
	}
	return nearWriter, nearReader, nearSvc, nil
}

func WrapStorageWithCache(
	ctx context.Context,
	config *DataAvailabilityConfig,
	storageService StorageService,
	lifecycleManager *LifecycleManager) (StorageService, error) {
	if storageService == nil {
		return nil, nil
	}

	// Enable caches, Redis and (local) Cache. Local is the outermost, so it will be tried first.
	var err error
	if config.RedisCache.Enable {
		storageService, err = NewRedisStorageService(config.RedisCache, storageService)
		lifecycleManager.Register(storageService)
		if err != nil {
			return nil, err
		}
	}
	if config.LocalCache.Enable {
		storageService = NewCacheStorageService(config.LocalCache, storageService)
		lifecycleManager.Register(storageService)
	}
	return storageService, nil
}

func CreateBatchPosterDAS(
	ctx context.Context,
	config *DataAvailabilityConfig,
	dataSigner signature.DataSignerFunc,
	l1Reader arbutil.L1Interface,
	sequencerInboxAddr common.Address,
) (DataAvailabilityServiceWriter, DataAvailabilityServiceReader, *KeysetFetcher, *LifecycleManager, error) {
	if !config.Enable {
		return nil, nil, nil, nil, nil
	}
	// Check config requirements
	if !config.RPCAggregator.Enable || !config.RestAggregator.Enable {
		return nil, nil, nil, nil, errors.New("--node.data-availability.rpc-aggregator.enable and rest-aggregator.enable must be set when running a Batch Poster in AnyTrust mode")
	}
	// Done checking config requirements

	var daWriter DataAvailabilityServiceWriter
	daWriter, err := NewRPCAggregator(ctx, *config, dataSigner)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	restAgg, err := NewRestfulClientAggregator(ctx, &config.RestAggregator)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	restAgg.Start(ctx)
	var lifecycleManager LifecycleManager
	lifecycleManager.Register(restAgg)
	var daReader DataAvailabilityServiceReader = restAgg
	keysetFetcher, err := NewKeysetFetcher(l1Reader, sequencerInboxAddr)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return daWriter, daReader, keysetFetcher, &lifecycleManager, nil
}

func CreateDAComponentsForDaserver(
	ctx context.Context,
	config *DataAvailabilityConfig,
	l1Reader *headerreader.HeaderReader,
	seqInboxAddress *common.Address,
) (DataAvailabilityServiceReader, DataAvailabilityServiceWriter, *SignatureVerifier, DataAvailabilityServiceHealthChecker, *LifecycleManager, error) {
	if !config.Enable {
		return nil, nil, nil, nil, nil, nil
	}

	// Check config requirements
	if !config.LocalDBStorage.Enable &&
		!config.LocalFileStorage.Enable &&
		!config.S3Storage.Enable {
		return nil, nil, nil, nil, nil, errors.New("At least one of --data-availability.(local-db-storage|local-file-storage|s3-storage) must be enabled.")
	}
	// Done checking config requirements

	storageService, dasLifecycleManager, err := CreatePersistentStorageService(ctx, config)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	storageService, err = WrapStorageWithCache(ctx, config, storageService, dasLifecycleManager)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	var daWriter DataAvailabilityServiceWriter
	var daReader DataAvailabilityServiceReader = storageService
	var daHealthChecker DataAvailabilityServiceHealthChecker = storageService

	if config.NEARAggregator.Enable {
		log.Info("Enabling NEAR aggregator")
		w, r, svc, err := CreateNearDAS(ctx, config, dasLifecycleManager)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		if config.NEARAggregator.StorageConfig.Enable {
			nearStorageService, err := NewNearStorageService(r, svc, config.NEARAggregator.StorageConfig)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			daReader = nearStorageService
			syncConf := config.NEARAggregator.StorageConfig.SyncToStorage
			var retentionPeriodSeconds uint64
			if uint64(syncConf.RetentionPeriod) == math.MaxUint64 {
				retentionPeriodSeconds = math.MaxUint64
			} else {
				retentionPeriodSeconds = uint64(syncConf.RetentionPeriod.Seconds())
			}

			if config.NEARAggregator.StorageConfig.SyncToStorage.Eager {
				if l1Reader == nil || seqInboxAddress == nil {
					return nil, nil, nil, nil, nil, errors.New("l1-node-url and sequencer-inbox-address must be specified along with sync-to-storage.eager")
				}
				storageService, err = NewSyncingFallbackStorageService(
					ctx,
					nearStorageService,
					storageService,
					storageService,
					l1Reader,
					*seqInboxAddress,
					&syncConf)
				if err != nil {
					return nil, nil, nil, nil, nil, err
				}
			} else {
				storageService = NewFallbackStorageService(nearStorageService, storageService, storageService,
					retentionPeriodSeconds, syncConf.IgnoreWriteErrors, true)
			}

			dasLifecycleManager.Register(storageService)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
		} else {
			daWriter = w
			daReader = r
		}
	} else {
		// The REST aggregator is used as the fallback if requested data is not present
		// in the storage service.
		if config.RestAggregator.Enable {
			restAgg, err := NewRestfulClientAggregator(ctx, &config.RestAggregator)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			restAgg.Start(ctx)
			dasLifecycleManager.Register(restAgg)

			syncConf := &config.RestAggregator.SyncToStorage
			var retentionPeriodSeconds uint64
			if uint64(syncConf.RetentionPeriod) == math.MaxUint64 {
				retentionPeriodSeconds = math.MaxUint64
			} else {
				retentionPeriodSeconds = uint64(syncConf.RetentionPeriod.Seconds())
			}

			if syncConf.Eager {
				if l1Reader == nil || seqInboxAddress == nil {
					return nil, nil, nil, nil, nil, errors.New("l1-node-url and sequencer-inbox-address must be specified along with sync-to-storage.eager")
				}
				storageService, err = NewSyncingFallbackStorageService(
					ctx,
					storageService,
					restAgg,
					restAgg,
					l1Reader,
					*seqInboxAddress,
					syncConf)
				dasLifecycleManager.Register(storageService)
				if err != nil {
					return nil, nil, nil, nil, nil, err
				}
			} else {
				storageService = NewFallbackStorageService(storageService, restAgg, restAgg,
					retentionPeriodSeconds, syncConf.IgnoreWriteErrors, true)
				dasLifecycleManager.Register(storageService)
			}

		}
	}
	// var daWriter DataAvailabilityServiceWriter
	// var daReader DataAvailabilityServiceReader = storageService
	// var daHealthChecker DataAvailabilityServiceHealthChecker = storageService
	var signatureVerifier *SignatureVerifier

	if config.Key.KeyDir != "" || config.Key.PrivKey != "" {
		var seqInboxCaller *bridgegen.SequencerInboxCaller
		if seqInboxAddress != nil {
			seqInbox, err := bridgegen.NewSequencerInbox(*seqInboxAddress, (*l1Reader).Client())
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			log.Info("Da reader/writer", "reader", daReader, "writer", daWriter)
			seqInboxCaller = &seqInbox.SequencerInboxCaller
		}
		if config.DisableSignatureChecking {
			seqInboxCaller = nil
		}

		daWriter, err = NewSignAfterStoreDASWriter(ctx, *config, storageService)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}

		signatureVerifier, err = NewSignatureVerifierWithSeqInboxCaller(
			seqInboxCaller,
			config.ExtraSignatureCheckingPublicKey,
		)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}

	return daReader, daWriter, signatureVerifier, daHealthChecker, dasLifecycleManager, nil
}

func CreateDAReaderForNode(
	ctx context.Context,
	config *DataAvailabilityConfig,
	l1Reader *headerreader.HeaderReader,
	seqInboxAddress *common.Address,
) (DataAvailabilityServiceReader, *KeysetFetcher, *LifecycleManager, error) {
	if !config.Enable {
		return nil, nil, nil, nil
	}

	// Check config requirements
	if config.RPCAggregator.Enable {
		return nil, nil, nil, errors.New("node.data-availability.rpc-aggregator is only for Batch Poster mode")
	}

	if !config.RestAggregator.Enable {
		return nil, nil, nil, fmt.Errorf("--node.data-availability.enable was set but not --node.data-availability.rest-aggregator. When running a Nitro Anytrust node in non-Batch Poster mode, some way to get the batch data is required.")
	}
	if !config.NEARAggregator.Enable {
		if !config.RestAggregator.Enable {
			return nil, nil, nil, fmt.Errorf("--node.data-availability.enable was set but neither of --node.data-availability.(rest-aggregator|ipfs-storage) were enabled. When running a Nitro Anytrust node in non-Batch Poster mode, some way to get the batch data is required.")
		}
	}
	// Done checking config requirements

	var lifecycleManager LifecycleManager
	var daReader DataAvailabilityServiceReader
	storageService, dasLifecycleManager, err := CreatePersistentStorageService(ctx, config)
	if err != nil {
		return nil, nil, nil, err
	}
	if config.NEARAggregator.Enable {
		log.Info("Initialising near service")
		_, r, _, err := CreateNearDAS(ctx, config, dasLifecycleManager)
		if err != nil {
			return nil, nil, nil, err
		}

		if storageService != nil {
			syncConf := &config.RestAggregator.SyncToStorage
			var retentionPeriodSeconds uint64
			if uint64(syncConf.RetentionPeriod) == math.MaxUint64 {
				retentionPeriodSeconds = math.MaxUint64
			} else {
				retentionPeriodSeconds = uint64(syncConf.RetentionPeriod.Seconds())
			}

			// This falls back to REST and updates the local IPFS repo if the data is found.
			nearSvc, err := NewNearServiceSidecar(*config)
			if err != nil {
				log.Error("initialising near service", "error", err)
				return nil, nil, nil, err
			}
			// TODO: hack fix
			storageService = NewFallbackStorageService(storageService, r, nearSvc,
				retentionPeriodSeconds, syncConf.IgnoreWriteErrors, true)
			dasLifecycleManager.Register(storageService)

			daReader = storageService
		} else {
			daReader = r
		}
	}
	if config.RestAggregator.Enable {
		var restAgg *SimpleDASReaderAggregator
		restAgg, err := NewRestfulClientAggregator(ctx, &config.RestAggregator)
		if err != nil {
			return nil, nil, nil, err
		}
		restAgg.Start(ctx)
		lifecycleManager.Register(restAgg)
		daReader = restAgg
	}

	var keysetFetcher *KeysetFetcher
	if seqInboxAddress != nil {
		seqInbox, err := bridgegen.NewSequencerInbox(*seqInboxAddress, (*l1Reader).Client())
		if err != nil {
			return nil, nil, nil, err
		}
		keysetFetcher, err = NewKeysetFetcherWithSeqInbox(seqInbox)
		if err != nil {
			return nil, nil, nil, err
		}

	}

	return daReader, keysetFetcher, &lifecycleManager, nil
}
