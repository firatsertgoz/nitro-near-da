package das

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	nearsc "github.com/near/rollup-data-availability/gopkg/sidecar"
	"github.com/offchainlabs/nitro/arbstate/daprovider"
	"github.com/offchainlabs/nitro/blsSignatures"
	flag "github.com/spf13/pflag"
)

// TODO: inject in place of RPC aggregator
// type NearService struct {
// 	near near.Config
// 	pub  blsSignatures.PublicKey
// 	priv blsSignatures.PrivateKey
// }

type NearServiceSidecar struct {
	nearsc.Client
	pub  blsSignatures.PublicKey
	priv blsSignatures.PrivateKey
}

func NewNearServiceSidecar(config DataAvailabilityConfig) (*NearServiceSidecar, error) {
	privKey, err := DecodeBase64BLSPrivateKey([]byte(config.Key.PrivKey))
	if err != nil {
		return nil, err
	}

	pubKey, err := blsSignatures.PublicKeyFromPrivateKey(privKey)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err

	}
	c, err := nearsc.NewClient("", &nearsc.ConfigureClientRequest{
		AccountID:  config.NEARAggregator.Account,
		ContractID: config.NEARAggregator.Contract,
		SecretKey:  config.NEARAggregator.Key,
		Network:    config.NEARAggregator.Network,
		Namespace:  &config.NEARAggregator.Namespace,
	})
	return &NearServiceSidecar{
		Client: *c,
		pub:    pubKey,
		priv:   privKey,
	}, nil
}

// func NewNearService(config DataAvailabilityConfig) (*NearService, error) {

// 	privKey, err := DecodeBase64BLSPrivateKey([]byte(config.Key.PrivKey))
// 	if err != nil {
// 		return nil, err
// 	}

// 	pubKey, err := blsSignatures.PublicKeyFromPrivateKey(privKey)
// 	if err != nil {
// 		return nil, err
// 	}

// 	near, err := near.NewConfig(config.NEARAggregator.Account, config.NEARAggregator.Contract, config.NEARAggregator.Key, config.NEARAggregator.Network, config.NEARAggregator.Namespace)

// 	return &NearService{
// 		near: *near,
// 		pub:  pubKey,
// 		priv: privKey,
// 	}, nil
// }

func (s *NearServiceSidecar) String() string {
	return fmt.Sprintf("NearService{}")
}

type NearAggregatorConfig struct {
	Enable        bool
	Account       string
	Contract      string
	Key           string
	Network       nearsc.Network
	Namespace     nearsc.Namespace
	StorageConfig NearStorageConfig `koanf:"storage"`
}

type NearStorageConfig struct {
	Enable        bool
	DataDir       string
	SyncToStorage SyncToStorageConfig `koanf:"sync-to-storage"`
}

// DefaultNearStorageConfig
var DefaultNearStorageConfig = NearStorageConfig{
	Enable:        true,
	DataDir:       "./target/output",
	SyncToStorage: DefaultSyncToStorageConfig,
}

func NearStorageConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".enable", DefaultNearStorageConfig.Enable, "enable storage of sequencer batch data from a list of RPC endpoints; if other DAS storage types are enabled, this mode is used as a fallback")
	f.String(prefix+".data-dir", DefaultNearStorageConfig.DataDir, "directory to store the storage data")
}

func NewNearAggregator(ctx context.Context, config DataAvailabilityConfig, nsvc *NearServiceSidecar) (*Aggregator, error) {
	svc := ServiceDetails{
		service:     (DataAvailabilityServiceWriter)(nsvc),
		pubKey:      nsvc.pub,
		signersMask: 1,
		metricName:  "near",
	}

	services := make([]ServiceDetails, 1)
	services[0] = svc

	return NewAggregator(ctx, config, services)
}

// Change from daprovider.DataAvailabilityCertificate to daprovider.DataAvailabilityCertificate
// Change to Store(ctx context.Context, message []byte, timeout uint64) (*daprovider.DataAvailabilityCertificate, error)
func (s *NearServiceSidecar) Store(ctx context.Context, message []byte, timeout uint64) (*daprovider.DataAvailabilityCertificate, error) {
	log.Info("Storing message", "message", message)
	Blob := nearsc.Blob{
		Data: message,
	}
	blobRef, err := s.Client.SubmitBlob(Blob)
	if err != nil {
		return nil, err
	}

	keysetHash, keySet, err := s.KeysetHash()
	if err != nil {
		log.Error("Error getting keyset hash", "err", err, "keyset", keySet)
		return nil, err
	}
	var certificate = daprovider.DataAvailabilityCertificate{
		KeysetHash:  keysetHash,
		DataHash:    [32]byte(blobRef.Deref()),
		Timeout:     timeout,
		Sig:         nil,
		SignersMask: 1,
		Version:     255,
	}
	newSig, err := blsSignatures.SignMessage(s.priv, certificate.SerializeSignableFields())
	if err != nil {
		return nil, err
	}
	certificate.Sig = newSig

	return &certificate, nil
}

func (s *NearServiceSidecar) KeysetHash() ([32]byte, []byte, error) {
	svc := ServiceDetails{
		service:     (DataAvailabilityServiceWriter)(s),
		pubKey:      s.pub,
		signersMask: 1,
		metricName:  "near",
	}

	services := make([]ServiceDetails, 1)
	services[0] = svc
	return KeysetHashFromServices(services, 1)
}

func (s *NearServiceSidecar) GetByHash(ctx context.Context, hash common.Hash) ([]byte, error) {
	log.Info("Getting message", "hash", hash)
	// Hack to bypass commitment
	bytesPadded := make([]byte, 64)
	copy(bytesPadded[0:32], hash.Bytes())
	blobRef, err := nearsc.NewBlobRef(bytesPadded)
	if err != nil {
		return nil, err
	}
	blob, err := s.Client.GetBlob(*blobRef)
	if err != nil {
		return nil, err
	}
	return blob.Data, nil

}

func (s *NearServiceSidecar) ExpirationPolicy(ctx context.Context) (daprovider.ExpirationPolicy, error) {
	return daprovider.KeepForever, nil
}

var DefaultNearAggregatorConfig = NearAggregatorConfig{
	Enable:   true,
	Account:  "topgunbakugo.testnet",
	Contract: "nitro.topgunbakugo.testnet",
	Key:      "ed25519:kjdshdfskjdfhsdk",
	Namespace: nearsc.Namespace{
		ID:      1,
		Version: 1,
	},
	StorageConfig: DefaultNearStorageConfig,
}

func NearAggregatorConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".enable", DefaultNearAggregatorConfig.Enable, "enable retrieval of sequencer batch data from a list of remote REST endpoints; if other DAS storage types are enabled, this mode is used as a fallback")
	f.String(prefix+".account", DefaultNearAggregatorConfig.Account, "Account Id for signing NEAR transactions")
	f.String(prefix+".contract", DefaultNearAggregatorConfig.Contract, "Contract address for submitting NEAR transactions")
	f.String(prefix+".key", DefaultNearAggregatorConfig.Key, "ED25519 Key for signing NEAR transactions, prefixed with 'ed25519:'")
	f.Uint32(prefix+".namespace", uint32(DefaultNearAggregatorConfig.Namespace.ID), "Namespace Id for this rollup")
	NearStorageConfigAddOptions(prefix+".storage", f)
}

// TODO: add healtchecks
func (s *NearServiceSidecar) HealthCheck(ctx context.Context) error {
	s.Client.Health()
	return nil
}

func (s *NearServiceSidecar) Put(ctx context.Context, data []byte, expirationTime uint64) error {
	Blob := nearsc.Blob{
		Data: data,
	}
	blobRef, err := s.Client.SubmitBlob(Blob)
	if err != nil {
		return err
	}
	log.Info("Blob ref", "blob ref", blobRef.ID())
	return nil
}

type NearStorageService struct {
	*NearServiceSidecar
	config NearStorageConfig
}

func NewNearStorageService(r DataAvailabilityServiceReader, nsvc *NearServiceSidecar, config NearStorageConfig) (*NearStorageService, error) {
	return &NearStorageService{
		NearServiceSidecar: nsvc,
		config:             config,
	}, nil
}

func (s *NearServiceSidecar) Sync(ctx context.Context) error {
	return nil
}

func (s *NearServiceSidecar) Close(ctx context.Context) error {
	return nil
}

func (s *NearServiceSidecar) Stringer() string {
	return fmt.Sprintf("NearService{}")
}

func (s *NearServiceSidecar) DASReader() daprovider.DASReader {
	return s
}

func (s *NearServiceSidecar) DASWriter() daprovider.DASWriter {
	return s
}
