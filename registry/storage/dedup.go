package storage

import (
	"fmt"
	"path"
	//	"strings"

	//	"github.com/opencontainers/go-digest"

	//NANNAN
	"io"
	//	"archive"
	"crypto/sha512"
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/archive"
	//"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/chrootarchive"
	"github.com/docker/docker/pkg/idtools"
)

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
//			2. directory,
//				sha256/hex[:1]/sha256digest/data ->
//				sha256/hex[:1]/sha256digest/layer_dirs_hierachy/link_to_uniq_file
//			===== file level dedup: table on redis memory ====
//			added two table:
//					1. index table:
//								 |					|						|
//								 | uniq_file digest	|	location_on_disk	|
//								 |					|						|
//					2. recipe table
//								 |					|								  |
//								 |	layer_digest	|   /path/to/file_name.extension  |
//								 |					|								  |
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

func DedupLayersFromPath(absPath string) error {
	//NANNAN: stat layerPath and read gzip file
	//var f io.Reader
	layerPath := path.Join("/var/lib/registry", absPath)
	fmt.Println("NANNAN: START DEDUPLICATION FROM PATH:%v", layerPath)
	//f, err := os.Open(layerPath)
	//if err != nil {
	//	fmt.Println(err)
	//	os.Exit(1)
	//}
	//	defer f.Close()
	//fmt.Println("NANNAN: start decompressing layer")
	//inflatedLayerData, err := archive.DecompressStream(f)
	//defer inflatedLayerData.Close()
	//if err != nil {
	//	fmt.Println("NANNAN: could not get decompression stream: %v", err)
	//	return err
	//}
	//Decompression
	parentDir := path.Dir(layerPath)
	unpackPath := path.Join(parentDir, "diff")
	
	archiver := archive.NewDefaultArchiver()
	options := &TarOptions{
		UIDMaps: archiver.IDMapping.UIDs(),
		GIDMaps: archiver.IDMapping.GIDs(),
	}
	idMapping := idtools.NewIDMappingsFromMaps(options.UIDMaps, options.GIDMaps)
	rootIDs := idMapping.RootPair()
	err = idtools.MkdirAllAndChownNew(unpackPath, 0777, rootIDs)
	if err != nil {
		return err
	}
	
	err := archiver.UntarPath(layerPath, unpackPath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	//parentDir := path.Dir(layerPath)
	//walk through directory
	//unpackPath := path.Join(parentDir, "diff")

	//	filepath.Walk
	err = filepath.Walk(unpackPath, checkDuplicate)
	if err != nil {
		log.Fatal(err)
	}
	return err
	//	gzf, err := gzip.NewReader(f)
	//	if err != nil{
	//		fmt.Println(err)
	//		os.Exit(1)
	//	}
	//
	//	tarReader := tar.NewReader(gzf)
	//
	//	i := 0
	//	for

	//NANNAN: dedup
}

var files = make(map[[sha512.Size]byte]string)

func checkDuplicate(path string, info os.FileInfo, err error) error {
	fmt.Printf("NANNAN: START CHECK DUPLICATE========>")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if info.IsDir() {
		return nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	digest := sha512.Sum512(data)
	if v, ok := files[digest]; ok {
		fmt.Printf("%q is a duplicate of %q\n", path, v)
	} else {
		//check redis file
		files[digest] = path
	}

	return nil
}

func applyDiff(layerPath string, diff io.Reader) error {
	//path := path.Join(layerPath, "diff")
	fmt.Println("NANNAN: start unpacking layer")
	parentDir := path.Dir(layerPath)
	diffPath := path.Join(parentDir, "diff")

	uidMap := []idtools.IDMap{
		{
			ContainerID: 0,
			HostID:      os.Getuid(),
			Size:        1,
		},
	}
	gidMap := []idtools.IDMap{
		{
			ContainerID: 0,
			HostID:      os.Getgid(),
			Size:        1,
		},
	}
	//	options := graphdriver.Options{Root: td, UIDMaps: uidMap, GIDMaps: gidMap}
	return chrootarchive.UntarUncompressed(diff, diffPath, &archive.TarOptions{
		UIDMaps: uidMap,
		GIDMaps: gidMap,
	})
}

//	uidMaps       []idtools.IDMap
//	gidMaps       []idtools.IDMap

// ApplyDiff extracts the changeset from the given diff into the
// layer with the specified id and parent, returning the size of the
// new layer in bytes.
//func ApplyDiff(id, parent string, diff io.Reader) (size int64, err error) {
//	if !a.isParent(id, parent) {
//		return a.naiveDiff.ApplyDiff(id, parent, diff)
//	}
//
//	// AUFS doesn't need the parent id to apply the diff if it is the direct parent.
//	if err = a.applyDiff(id, diff); err != nil {
//		return
//	}
//
//	return a.DiffSize(id, parent)
//}

//func (ls *layerStore) applyTar(tx *fileMetadataTransaction, ts io.Reader, parent string, layer *roLayer) error {
//	digester := digest.Canonical.Digester()
//	tr := io.TeeReader(ts, digester.Hash())
//
//	rdr := tr
//	if ls.useTarSplit {
//		tsw, err := tx.TarSplitWriter(true)
//		if err != nil {
//			return err
//		}
//		metaPacker := storage.NewJSONPacker(tsw)
//		defer tsw.Close()
//
//		// we're passing nil here for the file putter, because the ApplyDiff will
//		// handle the extraction of the archive
//		rdr, err = asm.NewInputTarStream(tr, metaPacker, nil)
//		if err != nil {
//			return err
//		}
//	}
//
//	applySize, err := ls.driver.ApplyDiff(layer.cacheID, parent, rdr)
//	if err != nil {
//		return err
//	}
//
//	// Discard trailing data but ensure metadata is picked up to reconstruct stream
//	io.Copy(ioutil.Discard, rdr) // ignore error as reader may be closed
//
//	layer.size = applySize
//	layer.diffID = DiffID(digester.Digest())
//
//	logrus.Debugf("Applied tar %s to %s, size: %d", layer.diffID, layer.cacheID, applySize)
//
//	return nil
//}

//// Diff produces an archive of the changes between the specified
//// layer and its parent layer which may be "".
//func Diff(id, parent string) (io.ReadCloser, error) {
////	if !a.isParent(id, parent) {
////		return a.naiveDiff.Diff(id, parent)
////	}
//	// AUFS doesn't need the parent layer to produce a diff.
//	return archive.TarWithOptions(path.Join(a.rootPath(), "diff", id), &archive.TarOptions{
//		Compression:     archive.Uncompressed,
//		ExcludePatterns: []string{archive.WhiteoutMetaPrefix + "*", "!" + archive.WhiteoutOpaqueDir},
//		UIDMaps:         a.uidMaps,
//		GIDMaps:         a.gidMaps,
//	})
//}
