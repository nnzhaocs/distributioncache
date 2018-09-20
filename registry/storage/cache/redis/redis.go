package redis

import (
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache"
	"github.com/garyburd/redigo/redis"
	"github.com/opencontainers/go-digest"
	//NANNAN
	"encoding/json"
	"flag"
	rejson "github.com/nitishm/go-rejson"
//	"log"

	"github.com/gomodule/redigo/redis"
)

// redisBlobStatService provides an implementation of
// BlobDescriptorCacheProvider based on redis. Blob descriptors are stored in
// two parts. The first provide fast access to repository membership through a
// redis set for each repo. The second is a redis hash keyed by the digest of
// the layer, providing path, length and mediatype information. There is also
// a per-repository redis hash of the blob descriptor, allowing override of
// data. This is currently used to override the mediatype on a per-repository
// basis.
//
// Note that there is no implied relationship between these two caches. The
// layer may exist in one, both or none and the code must be written this way.

var (
	dbNoBlob = 0 
	dbNoFile = 1
	dbNoBFRecipe = 2
)

	

type redisBlobDescriptorService struct {
	pool *redis.Pool

	// TODO(stevvooe): We use a pool because we don't have great control over
	// the cache lifecycle to manage connections. A new connection if fetched
	// for each operation. Once we have better lifecycle management of the
	// request objects, we can change this to a connection.
}

// NewRedisBlobDescriptorCacheProvider returns a new redis-based
// BlobDescriptorCacheProvider using the provided redis connection pool.
func NewRedisBlobDescriptorCacheProvider(pool *redis.Pool) cache.BlobDescriptorCacheProvider {
	return &redisBlobDescriptorService{
		pool: pool,
	}
}

// RepositoryScoped returns the scoped cache.
func (rbds *redisBlobDescriptorService) RepositoryScoped(repo string) (distribution.BlobDescriptorService, error) {
	if _, err := reference.ParseNormalizedNamed(repo); err != nil {
		return nil, err
	}

	return &repositoryScopedRedisBlobDescriptorService{
		repo:     repo,
		upstream: rbds,
	}, nil
}

// Stat retrieves the descriptor data from the redis hash entry.
func (rbds *redisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return distribution.Descriptor{}, err
	}

	return rbds.stat(ctx, conn, dgst)
}

func (rbds *redisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return err
	}

	// Not atomic in redis <= 2.3
	reply, err := conn.Do("HDEL", rbds.blobDescriptorHashKey(dgst), "digest", "length", "mediatype")
	if err != nil {
		return err
	}

	if reply == 0 {
		return distribution.ErrBlobUnknown
	}

	return nil
}

// stat provides an internal stat call that takes a connection parameter. This
// allows some internal management of the connection scope.
func (rbds *redisBlobDescriptorService) stat(ctx context.Context, conn redis.Conn, dgst digest.Digest) (distribution.Descriptor, error) {
	reply, err := redis.Values(conn.Do("HMGET", rbds.blobDescriptorHashKey(dgst), "digest", "size", "mediatype"))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
	if len(reply) < 3 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	var desc distribution.Descriptor
	if _, err := redis.Scan(reply, &desc.Digest, &desc.Size, &desc.MediaType); err != nil {
		return distribution.Descriptor{}, err
	}

	return desc, nil
}

// SetDescriptor sets the descriptor data for the given digest using a redis
// hash. A hash is used here since we may store unrelated fields about a layer
// in the future.
func (rbds *redisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return err
	}

	return rbds.setDescriptor(ctx, conn, dgst, desc)
}

func (rbds *redisBlobDescriptorService) setDescriptor(ctx context.Context, conn redis.Conn, dgst digest.Digest, desc distribution.Descriptor) error {
	if _, err := conn.Do("HMSET", rbds.blobDescriptorHashKey(dgst),
		"digest", desc.Digest,
		"size", desc.Size); err != nil {
		return err
	}

	// Only set mediatype if not already set.
	if _, err := conn.Do("HSETNX", rbds.blobDescriptorHashKey(dgst),
		"mediatype", desc.MediaType); err != nil {
		return err
	}

	return nil
}

func (rbds *redisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "blobs::" + dgst.String()
}

type repositoryScopedRedisBlobDescriptorService struct {
	repo     string
	upstream *redisBlobDescriptorService
}

var _ distribution.BlobDescriptorService = &repositoryScopedRedisBlobDescriptorService{}

// Stat ensures that the digest is a member of the specified repository and
// forwards the descriptor request to the global blob store. If the media type
// differs for the repository, we override it.
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	conn := rsrbds.upstream.pool.Get()
	//NANNAN
	defer conn.Close()
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return distribution.Descriptor{}, err
	}
	
	// Check membership to repository first
	member, err := redis.Bool(conn.Do("SISMEMBER", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if !member {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	upstream, err := rsrbds.upstream.stat(ctx, conn, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// We allow a per repository mediatype, let's look it up here.
	mediatype, err := redis.String(conn.Do("HGET", rsrbds.blobDescriptorHashKey(dgst), "mediatype"))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if mediatype != "" {
		upstream.MediaType = mediatype
	}

	return upstream, nil
}

// Clear removes the descriptor from the cache and forwards to the upstream descriptor store
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	conn := rsrbds.upstream.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return err
	}

	// Check membership to repository first
	member, err := redis.Bool(conn.Do("SISMEMBER", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst))
	if err != nil {
		return err
	}

	if !member {
		return distribution.ErrBlobUnknown
	}

	return rsrbds.upstream.Clear(ctx, dgst)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	if dgst != desc.Digest {
		if dgst.Algorithm() == desc.Digest.Algorithm() {
			return fmt.Errorf("redis cache: digest for descriptors differ but algorthim does not: %q != %q", dgst, desc.Digest)
		}
	}

	conn := rsrbds.upstream.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
//		defer conn.Close()
		return err
	}

	return rsrbds.setDescriptor(ctx, conn, dgst, desc)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) setDescriptor(ctx context.Context, conn redis.Conn, dgst digest.Digest, desc distribution.Descriptor) error {
	if _, err := conn.Do("SADD", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst); err != nil {
		return err
	}

	if err := rsrbds.upstream.setDescriptor(ctx, conn, dgst, desc); err != nil {
		return err
	}

	// Override repository mediatype.
	if _, err := conn.Do("HSET", rsrbds.blobDescriptorHashKey(dgst), "mediatype", desc.MediaType); err != nil {
		return err
	}

	// Also set the values for the primary descriptor, if they differ by
	// algorithm (ie sha256 vs sha512).
	if desc.Digest != "" && dgst != desc.Digest && dgst.Algorithm() != desc.Digest.Algorithm() {
		if err := rsrbds.setDescriptor(ctx, conn, desc.Digest, desc); err != nil {
			return err
		}
	}

	return nil
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "repository::" + rsrbds.repo + "::blobs::" + dgst.String()
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) repositoryBlobSetKey(repo string) string {
	return "repository::" + rsrbds.repo + "::blobs"
}


//NANNAN: for deduplication
//var RedisPool *redis.Pool

type redisFileDescriptorService struct {
	pool *redis.Pool

	// TODO(stevvooe): We use a pool because we don't have great control over
	// the cache lifecycle to manage connections. A new connection if fetched
	// for each operation. Once we have better lifecycle management of the
	// request objects, we can change this to a connection.
}

// NewRedisBlobDescriptorCacheProvider returns a new redis-based
// BlobDescriptorCacheProvider using the provided redis connection pool.
func NewRedisFileDescriptorCacheProvider(pool *redis.Pool) cache.FileDescriptorCacheProvider {
	return &redisFileDescriptorService{
		pool: pool,
	}
}

//"files::sha256:7173b809ca12ec5dee4506cd86be934c4596dd234ee82c0662eac04a8c2c71dc"
func (rfds *redisFileDescriptorService) fileDescriptorHashKey(dgst digest.Digest) string {
	return "files::" + dgst.String()
}
//func (rbds *redisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
//	return "blobs::" + dgst.String()
//}

//type repositoryScopedRedisBlobDescriptorService struct {
//	repo     string
//	upstream *redisBlobDescriptorService
//}

var _ distribution.FileDescriptorService = &redisFileDescriptorService{}

//func (rsrbds *repositoryScopedRedisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
//	return "repository::" + rsrbds.repo + "::blobs::" + dgst.String()
//}
//
//func (rsrbds *repositoryScopedRedisBlobDescriptorService) repositoryBlobSetKey(repo string) string {
//	return "repository::" + rsrbds.repo + "::blobs"
//}
//
//type repositoryScopedRedisBlobDescriptorService struct {
//	repo     string
//	upstream *redisBlobDescriptorService
//}

//func (cbds *redisFileDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
//	func (cbds *cachedBlobStatter) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
//}
//func (rfds *redisFileDescriptorService) StatFileDesriptor(dgst digest.Digest) (distribution.FileDescriptor, error) {
//	func (rfds *redisFileDescriptorService) SetFileDescriptor(dgst digest.Digest, desc distribution.FileDescriptor) error {
//}	

func (rfds *redisFileDescriptorService) StatFile(ctx context.Context, dgst digest.Digest) (distribution.FileDescriptor, error) {
	conn := rfds.pool.Get()
	defer conn.Close()
	
	if _, err := conn.Do("SELECT", dbNoFile); err != nil {
//		defer conn.Close()
		return distribution.FileDescriptor{}, err
	}
    
    reply, err := redis.Values(conn.Do("HMGET", rfds.fileDescriptorHashKey(dgst), "digest", "filePath"))
    	//, "fileSize", "layerDescriptor"
	if err != nil {
		return distribution.FileDescriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
	if len(reply) < 2 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
		return distribution.FileDescriptor{}, distribution.ErrBlobUnknown
	}

	var desc distribution.FileDescriptor
	if _, err = redis.Scan(reply, &desc.Digest, &desc.FilePath); err != nil {
		return distribution.FileDescriptor{}, err
	}

	return desc, nil
}

func (rfds *redisFileDescriptorService) SetFileDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.FileDescriptor) error {
	
	conn := rfds.pool.Get()
	defer conn.Close()
	
	if _, err := conn.Do("SELECT", dbNoFile); err != nil {
//		defer conn.Close()
		return err
	}
	
	if _, err := conn.Do("HMSET", rfds.fileDescriptorHashKey(dgst),
		"digest", desc.Digest,
		"filePath", desc.FilePath); err != nil {
		return err
	}

//	// Only set mediatype if not already set.
//	if _, err := conn.Do("HSETNX", rbds.blobDescriptorHashKey(dgst),
//		"mediatype", desc.MediaType); err != nil {
//		return err
//	}

	return nil
}

//"files::sha256:7173b809ca12ec5dee4506cd86be934c4596dd234ee82c0662eac04a8c2c71dc"
func (rfds *redisFileDescriptorService) BFRecipeHashKey(dgst digest.Digest) string {
	return "Blob:File:Recipe::" + dgst.String()
}

func (rfds *redisFileDescriptorService) StatBFRecipe(ctx context.Context, dgst digest.Digest) (desc distribution.BFRecipeDescriptor, error) {
	conn := rfds.pool.Get()
	defer conn.Close()
	
	if _, err := conn.Do("SELECT", dbNoBFRecipe); err != nil {
//		defer conn.Close()
		return distribution.BFRecipeDescriptor{}, err
	}
    
//    reply, err := redis.Values(conn.Do("HMGET", rfds.BFRecipeHashKey(dgst), "blobdigest", "filedescriptors"))
//    	//, "fileSize", "layerDescriptor"
//	if err != nil {
//		return distribution.BFRecipeDescriptor{}, err
//	}

	bfrJSON, err := redis.Bytes(rejson.JSONGet(conn, rfds.BFRecipeHashKey(dgst),
	""))
	if err != nil{
		return distribution.BFRecipeDescriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
//	if len(reply) < 2 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
//		return distribution.BFRecipeDescriptor{}, distribution.ErrBlobUnknown
//	}

//	var desc distribution.FileDescriptor
//	if _, err = redis.Scan(reply, &desc.BlobDigest, &desc.FileDescriptor); err != nil {
//		return distribution.BFRecipeDescriptor{}, err
//	}

	desc := distribution.BFRecipeDescriptor{}
	err = json.Unmarshal(bfrJSON, &desc)
	if err != nil{
		return distribution.BFRecipeDescriptor{}, distribution.ErrBlobUnknown
	}
	

	return desc, nil
}

`
type BFRecipeDescriptor struct{

	BlobDigest      Digest.digest
	BFDescriptors   []distribution.BFDescriptor
}

type BFDescriptor struct{

	BlobFilePath    string
	Digest          Digest.digest,
	DigestFilePath  string	
}
`

func (rfds *redisFileDescriptorService) SetBFRecipe(ctx context.Context, dgst digest.Digest, desc distribution.BFRecipeDescriptor) error {
	
	conn := rfds.pool.Get()
	defer conn.Close()
	
	if _, err := conn.Do("SELECT", dbNoBFRecipe); err != nil {
//		defer conn.Close()
		return err
	}
	
//	if _, err := conn.Do("HMSET", rfds.BFRecipeHashKey(dgst),
//		"blobdigest", desc.BlobDigest,
//		"bfdescriptors", desc.BFDescriptors); err != nil {
//		return err
//	}

	//NANNAN: use re-json
	
	res, err := rejson.JSONSet(conn, rfds.BFRecipeHashKey(dgst), 
	".", 
	desc, false, false)
	if err != nil{
		return err
	}

//	// Only set mediatype if not already set.
//	if _, err := conn.Do("HSETNX", rbds.blobDescriptorHashKey(dgst),
//		"mediatype", desc.MediaType); err != nil {
//		return err
//	}

	return nil
}
