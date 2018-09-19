//package storage
//
//import (
//////	"fmt"
//////	"path"
//////	"strings"
////
////	//NANNAN
//////	"io"//	"archive"
//////	"crypto/sha512"
//////	"io/ioutil"
//////	"os"
//////	"path/filepath"
////	
//////	"github.com/docker/distribution"
//////	log "github.com/Sirupsen/logrus"
//////	"github.com/docker/docker/pkg/archive"
//////	"github.com/docker/docker/pkg/idtools"
//////	rediscache "github.com/docker/distribution/registry/storage/cache/redis"
//////	redisDedup "github.com/docker/distribution/registry/storage/cache/redisDedup"
//////	"github.com/garyburd/redigo/redis"
//////	"github.com/docker/distribution/configuration"
//////	"github.com/docker/distribution"
//	"github.com/opencontainers/go-digest"
//)

//NANNAN: dedup operation
//once a layer or manifest is recieved, do dedup:
// read tarfile -> decompress -> unpack -> file digest -> check redis index table ->
// if not -> save, -> update redis index table
// else: -> drop
// update -> redis layer recipe
//
//			==== file level dedup: on disk storage ===
//			1. directory hierarchy, where unique files are saved
//				file_sha256/hex[:1]/sha256digest/filename.extension
//
//			2. directory,
//				sha256/hex[:1]/sha256digest/data -> removed
//				sha256/hex[:1]/sha256:digest/diff/path/to/uniqFile.extension
//
//			===== file level dedup: table on redis memory ====
//			added two table:
//					1. fileDescriptor table:
//								 |					|						|
//					files:digest |   file digest	|	location_on_disk	|
//								 |					|						|
//					2. recipe table
//								 |					|								  |
//			 layers:layer_digest |	 layer digest	|   /path/to/uniqFile.extension   |   file digest 
//								 |					|								  |
//
//			====== file level dedup: cache on redis memory ====
//					1. cache index table:
//					2. see if redis memory can store all recipe table
//								 |					|						|
//								 | uniq_file digest	|	location_on_disk	|
//								 |					|						|
//					3. cache files and cache layers
//
//
//

//func DedupLayersFromPath(absPath string) error {
//	layerPath := path.Join("/var/lib/registry", absPath)
//	
//	fmt.Println("NANNAN: START DEDUPLICATION FROM PATH :=>", layerPath)
//
//	parentDir := path.Dir(layerPath)
//	unpackPath := path.Join(parentDir, "diff")
//
//	archiver := archive.NewDefaultArchiver()
//	options := &archive.TarOptions{
//		UIDMaps: archiver.IDMapping.UIDs(),
//		GIDMaps: archiver.IDMapping.GIDs(),
//	}
//	idMapping := idtools.NewIDMappingsFromMaps(options.UIDMaps, options.GIDMaps)
//	rootIDs := idMapping.RootPair()
//	err := idtools.MkdirAllAndChownNew(unpackPath, 0777, rootIDs)
//	if err != nil {
//		return err
//	}
//
//	err = archiver.UntarPath(layerPath, unpackPath)
//	if err != nil {
//		fmt.Println(err)
//		return err
//	}
//	
//	err = filepath.Walk(unpackPath, checkDuplicate)
//	if err != nil {
//		log.Fatal(err)
//	}
//	return err
//}



//dbnoFileDidgest := 1
//
//


//func addFileMap(path string, )






