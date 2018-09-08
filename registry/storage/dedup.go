package storage

import (
	"fmt"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
	
	//NANNAN
	"io"
	"os"
	"compress/gzip"
	"archive/tar"
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

func DedupLayersFromPath(layerPath string) (error){
	//NANNAN: stat layerPath and read gzip file
	f, err := os.Open(layerPath)
	if err != nil{
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()
	
	gzf, err := gzip.NewReader(f)
	if err != nil{
		fmt.Println(err)
		os.Exit(1)
	}
	
	tarReader := tar.NewReader(gzf)
	
	i := 0
	for
	
	//NANNAN: dedup
	


}
