package das

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	near "github.com/near/rollup-data-availability/gopkg/da-rpc"
	"github.com/offchainlabs/nitro/arbstate/daprovider"
	"github.com/offchainlabs/nitro/blsSignatures"
	flag "github.com/spf13/pflag"
)

// TODO: inject in place of RPC aggregator
type NearService struct {
	near near.Config
	pub  blsSignatures.PublicKey
	priv blsSignatures.PrivateKey
}

func NewNearService(config DataAvailabilityConfig) (*NearService, error) {

	privKey, err := DecodeBase64BLSPrivateKey([]byte(config.Key.PrivKey))
	if err != nil {
		return nil, err
	}

	pubKey, err := blsSignatures.PublicKeyFromPrivateKey(privKey)
	if err != nil {
		return nil, err
	}

	near, err := near.NewConfig(config.NEARAggregator.Account, config.NEARAggregator.Contract, config.NEARAggregator.Key, config.NEARAggregator.Network, config.NEARAggregator.Namespace)

	return &NearService{
		near: *near,
		pub:  pubKey,
		priv: privKey,
	}, nil
}

func (s *NearService) String() string {
	return fmt.Sprintf("NearService{}")
}

type NearAggregatorConfig struct {
	Enable        bool
	Account       string
	Contract      string
	Key           string
	Network       string
	Namespace     uint32
	StorageConfig NearStorageConfig `koanf:"storage"`
}

type NearStorageConfig struct {
	Enable        bool
	DataDir       string
	SyncToStorage SyncToStorageConfig `koanf:"sync-to-storage"`
}

// DefaultNearStorageConfig
var DefaultNearStorageConfig = NearStorageConfig{
	Enable:  true,
	DataDir: "./target/output",
	SyncToStorage: DefaultSyncToStorageConfig,
}

func NearStorageConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".enable", DefaultNearStorageConfig.Enable, "enable storage of sequencer batch data from a list of RPC endpoints; if other DAS storage types are enabled, this mode is used as a fallback")
	f.String(prefix+".data-dir", DefaultNearStorageConfig.DataDir, "directory to store the storage data")
}

func NewNearAggregator(ctx context.Context, config DataAvailabilityConfig, nsvc *NearService) (*Aggregator, error) {
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
func (s *NearService) Store(ctx context.Context, message []byte, timeout uint64) (*daprovider.DataAvailabilityCertificate, error) {
	log.Info("Storing message", "message", message)
	blobRefBytes, err := s.near.ForceSubmit(message)
	if err != nil {
		return nil, err
	}
	blobRef := near.BlobRef{}
	err = blobRef.UnmarshalBinary(blobRefBytes)
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
		DataHash:    [32]byte(blobRef.TxId),
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

func (s *NearService) KeysetHash() ([32]byte, []byte, error) {
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

func (s *NearService) GetByHash(ctx context.Context, hash common.Hash) ([]byte, error) {
	log.Info("Getting message", "hash", hash)
	// Hack to bypass commitment
	bytesPadded := make([]byte, 64)
	copy(bytesPadded[0:32], hash.Bytes())
	bytes, err := s.near.Get(bytesPadded, 0)
	if err != nil {
		return nil, err
	}
	return bytes, nil

}

func (s *NearService) ExpirationPolicy(ctx context.Context) (daprovider.ExpirationPolicy, error) {
	return daprovider.KeepForever, nil
}

var DefaultNearAggregatorConfig = NearAggregatorConfig{
	Enable:        true,
	Account:       "topgunbakugo.testnet",
	Contract:      "nitro.topgunbakugo.testnet",
	Key:           "ed25519:kjdshdfskjdfhsdk",
	Namespace:     1,
	StorageConfig: DefaultNearStorageConfig,
}

func NearAggregatorConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".enable", DefaultNearAggregatorConfig.Enable, "enable retrieval of sequencer batch data from a list of remote REST endpoints; if other DAS storage types are enabled, this mode is used as a fallback")
	f.String(prefix+".account", DefaultNearAggregatorConfig.Account, "Account Id for signing NEAR transactions")
	f.String(prefix+".contract", DefaultNearAggregatorConfig.Contract, "Contract address for submitting NEAR transactions")
	f.String(prefix+".key", DefaultNearAggregatorConfig.Key, "ED25519 Key for signing NEAR transactions, prefixed with 'ed25519:'")
	f.Uint32(prefix+".namespace", DefaultNearAggregatorConfig.Namespace, "Namespace for this rollup")
	NearStorageConfigAddOptions(prefix+".storage", f)
}

// TODO: add healtchecks
func (s *NearService) HealthCheck(ctx context.Context) error {
	s.near.Get(nil, 0)
	return nil
}

func (s *NearService) Put(ctx context.Context, data []byte, expirationTime uint64) error {
	log.Info("Storing message", "message", data)
	frameRefBytes, err := s.near.ForceSubmit(data)
	if err != nil {
		return err
	}
	log.Info("Frame ref", "frame ref", hex.EncodeToString(frameRefBytes))
	return nil
}
type NearStorageService struct {
	*NearService
	config NearStorageConfig
}

func NewNearStorageService(r DataAvailabilityServiceReader, nsvc *NearService, config NearStorageConfig) (*NearStorageService, error) {
	return &NearStorageService{
		NearService: nsvc,
		config:      config,
	}, nil
}

func (s *NearService) Sync(ctx context.Context) error {
	return nil
}

func (s *NearService) Close(ctx context.Context) error {
	return nil
}

func (s *NearService) Stringer() string {
	return fmt.Sprintf("NearService{}")
}

func (s *NearService) DASReader() daprovider.DASReader {
	return s
}

func (s *NearService) DASWriter() daprovider.DASWriter {
	return s
}

