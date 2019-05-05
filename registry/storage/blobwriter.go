package storage

import (
	"errors"
	"fmt"
	"io"
	"path"
	"time"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
//NANNAN	
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/idtools"
	"path/filepath"
	"github.com/docker/distribution/registry/storage/cache"
	"os"
	"sync"
	//"path/filepath"
//	"github.com/serialx/hashring"
	"io/ioutil"
	"bytes"
	"net/http"
	//"regexp"
	redisgo"github.com/go-redis/redis"
	"math/rand"
	"strconv"
)

//NANNAN: TODO LIST
//1. when storing to recipe, remove prefix-:/var/lib/registry/docker/registry/v2/blobs/sha256/ for redis space savings.

var (
	errResumableDigestNotAvailable = errors.New("resumable digest not available")
	//NANNAN
	algorithm   = digest.Canonical
)

const (
	// digestSha256Empty is the canonical sha256 digest of empty data
	digestSha256Empty = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// blobWriter is used to control the various aspects of resumable
// blob upload.
type blobWriter struct {
	ctx       context.Context
	blobStore *linkedBlobStore

	id        string
	startedAt time.Time
	digester  digest.Digester
	written   int64 // track the contiguous write
	//NANNAN: filewriter
	fileWriter storagedriver.FileWriter
	driver     storagedriver.StorageDriver
	path       string

	resumableDigestEnabled bool
	committed              bool
}

var _ distribution.BlobWriter = &blobWriter{}

// ID returns the identifier for this upload.
func (bw *blobWriter) ID() string {
	return bw.id
}

func (bw *blobWriter) StartedAt() time.Time {
	return bw.startedAt
}

// Commit marks the upload as completed, returning a valid descriptor. The
// final size and digest are checked against the first descriptor provided.
func (bw *blobWriter) Commit(ctx context.Context, desc distribution.Descriptor) (distribution.Descriptor, error) {
	context.GetLogger(ctx).Debug("(*blobWriter).Commit")

	if err := bw.fileWriter.Commit(); err != nil {
		return distribution.Descriptor{}, err
	}

	bw.Close()
	desc.Size = bw.Size()

	canonical, err := bw.validateBlob(ctx, desc)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if err := bw.moveBlob(ctx, canonical); err != nil {
		return distribution.Descriptor{}, err
	}

	if err := bw.blobStore.linkBlob(ctx, canonical, desc.Digest); err != nil {
		return distribution.Descriptor{}, err
	}

	if err := bw.removeResources(ctx); err != nil {
		return distribution.Descriptor{}, err
	}

	err = bw.blobStore.blobAccessController.SetDescriptor(ctx, canonical.Digest, canonical)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	bw.committed = true
	return canonical, nil
}

//NANNAN: utility function. to remove duplicate ips from serverips
func RemoveDuplicateIpsFromIps(s []string) []string {
      m := make(map[string]bool)
      for _, item := range s {
              if _, ok := m[item]; ok {
                      // duplicate item
                      fmt.Println(item, "is a duplicate")
              } else {
                      m[item] = true
              }
      }

      var result []string
      for item, _ := range m {
              result = append(result, item)
      }
      return result
}

func getGID() float64{
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Float64()
}

type Pair struct{
	first string
	second string
}


/*
This function is used to forward put requests on to other registries 

DEBU[0021] authorizing request                           go.version=go1.12 http.request.host="localhost:5000" 
http.request.id=d8119314-5400-4477-bee9-ca4f2d808d52 
http.request.method=PUT http.request.remoteaddr="172.17.0.1:55964" 
http.request.uri="/v2/nnzhaocs/hello-world/blobs/uploads/736b54f9-38cb-4498-904c-a28b684c1a1c?_state=4zFr9emRBkO_Ij5iKV4y8GEtYwEpsMD3Z-M3x31jbRF7Ik5hbWUiOiJubnpoYW9jcy9oZWxsby13b3JsZCIsIlVVSUQiOiI3MzZiNTRmOS0zOGNiLTQ0OTgtOTA0Yy1hMjhiNjg0YzFhMWMiLCJPZmZzZXQiOjk3NywiU3RhcnRlZEF0IjoiMjAxOS0wNC0xNVQwMjoyODoxNloifQ%3D%3D&digest=sha256%3A1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced" 
http.request.useragent="docker/18.09.3 go/go1.10.8 git-commit/774a1f4 kernel/3.10.0-693.11.6.el7_lustre.x86_64 os/linux arch/amd64 UpstreamClient(Docker-Client/18.09.3 \\(linux\\))" vars.name="nnzhaocs/hello-world" vars.uuid=736b54f9-38cb-4498-904c-a28b684c1a1c

ERRO[0109] response completed with error                 
err.code="digest invalid" err.detail="digest parsing failed" 
err.message="provided digest did not match uploaded content" go.version=go1.12 http.request.host="192.168.0.215:5000" 
http.request.id=4af0c21c-315e-42f9-b04d-a99f0a6e6eac 
http.request.method=PUT http.request.remoteaddr="192.168.0.210:48070" 
http.request.uri="/v2/forward_repo/blobs/uploads/d8a15122-8119-4290-a100-bd6ccd4ce747?_state=8Jv7qNOl5I8kKqVzT3sst5mCBPTBdu8kH8v2Wzhvt6N7Ik5hbWUiOiJmb3J3YXJkX3JlcG8iLCJVVUlEIjoiZDhhMTUxMjItODExOS00MjkwLWExMDAtYmQ2Y2NkNGNlNzQ3IiwiT2Zmc2V0IjowLCJTdGFydGVkQXQiOiIyMDE5LTA0LTE1VDAyOjI4OjE2LjA5NTIzNTI2N1oifQ%3D%3D&digest=sha256%3Asha256:729a6da29d6e10228688fc0cf3e943068b459ef6f168afbbd2d3d44ee0f2fd01" 
http.request.useragent="Go-http-client/1.1" http.response.contenttype="application/json; charset=utf-8" http.response.duration=9.048157ms 
http.response.status=400 http.response.written=131 vars.name="forward_repo" vars.uuid=d8a15122-8119-4290-a100-bd6ccd4ce747
*/
func (bw *blobWriter) ForwardToRegistry(ctx context.Context, fpath string, wg *sync.WaitGroup) error {
	
	defer wg.Done()
	
	regname := filepath.Base(filepath.Dir(fpath)) 
	var regnamebuffer bytes.Buffer
	regnamebuffer.WriteString(regname)
	regnamebuffer.WriteString(":5000")
	regname = regnamebuffer.String()
	///var/lib/registry/docker/registry/v2/mv_tmp_serverfiles/192.168.0.200/mv_tar.tar.gz
	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry forwarding %s to %s", fpath, regname)
	var buffer bytes.Buffer
	buffer.WriteString("http://")
	buffer.WriteString(regname)
	buffer.WriteString("/v2/test_repo/blobs/sha256:")
	
	fp, err := os.Open(fpath) 
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry %s", err)
		return nil
	}
	
	defer fp.Close()
	
	digestFn := algorithm.FromReader
	dgst, err := digestFn(fp)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry %s: %v", fpath, err)
		return err
	}
	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry File %s dgest %s", fpath, dgst.String())
	dgststring := dgst.String()
	dgststring = strings.SplitN(dgststring, "sha256:", 2)[1]

	buffer.WriteString(dgststring)
	url := buffer.String()
	
	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry URL %s", url)

//let's skip head request
	//Send Get Request
//	head, err := http.Head(url)
//	if err != nil {
//		return err
//	}
//	
//	head.Body.Close()
//	if head.StatusCode == 404 {
	buffer.Reset()
	buffer.WriteString("http://")
	buffer.WriteString(regname)
	buffer.WriteString("/v2/forward_repo/forward_repo/blobs/uploads/")
	url = buffer.String()

	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry POST URL %s", url)
	post, err := http.Post(url, "*/*", nil)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry POST URL %s, err %s", url, err)
		return err
	}
	post.Body.Close()

	location := post.Header.Get("location")
	buffer.Reset()
	buffer.WriteString(location)
	buffer.WriteString("&digest=sha256%3A")
	buffer.WriteString(dgststring)
	url = buffer.String()
	fi, err := os.Stat(fpath)
	if err != nil {
		return err
	}
	file, err := os.Open(fpath)

	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry PUT URL %s", url)

	request, err := http.NewRequest("PUT", url, file)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry PUT URL %s, err %s", url, err)
		return err
	}

	request.ContentLength = fi.Size()
	client := &http.Client{}
	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry: Do(request) to %v", regname)
	put, err := client.Do(request)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry PUT URL %s, err %s", url, err)
		return err
	}
	if put.StatusCode < 200 || put.StatusCode > 299 {
		context.GetLogger(ctx).Errorf("%s returned status code %d", regname, put.StatusCode)
		return errors.New("put unique files to other servers, failed")
	}
	put.Body.Close()
	
	context.GetLogger(ctx).Debug("NANNAN: ForwardToRegistry remove files %s", url)
	
	//remove or not to remove? no diff
	if err = os.Remove(fpath); err != nil{
		context.GetLogger(ctx).Errorf("NANNAN: cannot remove fpath %s: %s", fpath, err)
		return err
	}
	tmp_dir := filepath.Base(fpath)
	if err = os.RemoveAll(path.Join("/var/lib/registry", "/docker/registry/v2/mv_tmp_serverfiles", regname, tmp_dir)); err != nil{
		context.GetLogger(ctx).Errorf("NANNAN: cannot remove all file in NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL: %s: %s", 
			path.Join("/var/lib/registry", "/docker/registry/v2/mv_tmp_serverfiles", regname, tmp_dir), err)
		return err	
	}

//	}

	return nil
}

// prepare forward files to other servers/registries
// FIRST, READ "/docker/registry/v2/blobs/sha256/1b/
// 1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/
// diff/8b6566f585bad55b6fb9efb1dc1b6532fd08bb1796b4b42a3050aacb961f1f3f"
// SECOND, store in "/docker/registry/v2/mv_tmp_serverfiles/192.168.0.200/tmp_dir/
// NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/
// 1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/diff/8b6566f585bad55b6fb9efb1dc1b6532fd08bb1796b4b42a3050aacb961f1f3f"
// THEN, compress as gizp files "/var/lib/registry/docker/registry/v2/mv_tmp_serverfiles/192.168.0.200/mv_tar.tar.gz"

func (bw *blobWriter)PrepareForward(ctx context.Context, serverForwardMap map[string][]string, gid float64) ([]string, error){
	var serverFiles []Pair
	limChan := make(chan bool, len(serverForwardMap))
	defer close(limChan)
	context.GetLogger(ctx).Debug("NANNAN: PrepareForward: [len(serverForwardMap)]=>%d", len(serverForwardMap))
	for i := 0; i < len(serverForwardMap); i++ {
		limChan <- true
	}
//	mvtarpathall := path.Join("/var/lib/registry", server)
	for server, fpathlst := range serverForwardMap{
		context.GetLogger(ctx).Debug("NANNAN: serverForwardMap: [%s]=>%", server, fpathlst)
		for _, fpath := range fpathlst{
			sftmp := Pair{
				first: server,
				second: fpath,
	//			DigestFilePath: dfp,
	//			ServerIp: server,//serverIp,
			}
			serverFiles = append(serverFiles, sftmp)
		}	
	}
	if tmp_dir, err := strconv.ParseFloat(gid, 64); err == nil {
//	    fmt.Println(s) // 3.14159265
		context.GetLogger(ctx).Debug("NANNAN: PrepareForward: the gid for this goroutine: =>%", tmp_dir)
	}

	for _, sftmp := range serverFiles{
		<-limChan
		go func(sftmp Pair){
			server := sftmp.first
//			context.GetLogger(ctx).Debug("NANNAN: PrepareForward: cping files to server [%s]", server)
			fpath := sftmp.second
//			reg, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
			tmpath := path.Join(server, tmp_dir, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL")
//			tarfpath := reg.ReplaceAllString(strings.SplitN(fpath, "diff", 2)[1], "") 
//			blobdgst := filepath.Base(strings.SplitN(fpath, "diff", 2)[0])
			//192.168.210/tmp_dir/NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/
			//898c46f3b1a1f39827ed135f020c32e2038c87ae0690a8fe73d94e5df9e6a2d6/
			//diff/bin/1c48ade64b96409e6773d2c5c771f3b3c5acec65a15980d8dca6b1efd3f95969
			withtmptarfpath := path.Join(tmpath, strings.TrimPrefix(fpath, "/var/lib/registry"))
//			context.GetLogger(ctx).Debug("NANNAN: withtmptarfpath: [%v]", withtmptarfpath)
			
			destfpath := path.Join("/docker/registry/v2/mv_tmp_serverfiles/", withtmptarfpath)
//			context.GetLogger(ctx).Debug("NANNAN: PrepareForward: cping files to server [%s], destfpath: [%s]", server, destfpath)
			
			contents, err := bw.driver.GetContent(ctx, strings.TrimPrefix(fpath, "/var/lib/registry")) 
			if err != nil {
				context.GetLogger(ctx).Errorf("NANNAN: CANNOT READ File FOR SENDING TO OTHER SERVER %s, ", err)
//				limChan <- true
			}else{
				err = bw.driver.PutContent(ctx, destfpath, contents)
				if err != nil {
					context.GetLogger(ctx).Errorf("NANNAN: CANNOT WRITE FILE TO DES DIR FOR SENDING TO OTHER SERVER %s, ", err)
				}
				//delete the old one
				err = bw.driver.Delete(ctx, strings.TrimPrefix(fpath, "/var/lib/registry"))
				if err != nil {
					context.GetLogger(ctx).Errorf("NANNAN: CANNOT DELETE THE ORGINIAL File FOR SENDING TO OTHER SERVER %s, ", err)
//				limChan <- true
				}
//					limChan <- true
//					}else{
////						limChan <- true
//					}
			}
			limChan <- trues
		}(sftmp)		
	} 
	// leave the errChan
	for i := 0; i < cap(limChan); i++{
		<-limChan 
		context.GetLogger(ctx).Debug("NANNAN: FORWARD <copy files> [%d th] goroutine is joined ", i) 
	}
	// all goroutines finished here
	
	context.GetLogger(ctx).Debug("NANNAN: FORWARD <copy files> all goroutines finished here") // not locally available
	
	for i := 0; i < len(serverForwardMap); i++ {
		limChan <- true
	}
	
	tarpathChan := make(chan string, len(serverForwardMap))
	errChan := make(chan error, len(serverForwardMap))
	defer close(tarpathChan)
	defer close(errChan)
	context.GetLogger(ctx).Debug("NANNAN: PrepareCompress: [len(serverForwardMap)]=>%d", len(serverForwardMap))
	for server, _ := range serverForwardMap{
		<-limChan
		context.GetLogger(ctx).Debug("NANNAN: PrepareCompress: compress files before sending to server [%s] ", server)
		go func(server string){
//			//tmpath := path.Join(server, "tmp_dir")
			packpath := path.Join("/var/lib/registry", "/docker/registry/v2/mv_tmp_serverfiles", server, tmp_dir) //tmp_dir is with gid
			context.GetLogger(ctx).Debug("NANNAN: PrepareCompress <COMPRESS> packpath: %s", packpath)
			
			data, err := archive.Tar(packpath, archive.Gzip)
			if err != nil {
				//TODO: process manifest file
				context.GetLogger(ctx).Errorf("NANNAN: Compress <COMPRESS tar> %s, ", err)
				errChan <- err
			}else{
			
				defer data.Close()
				
				packFile, err := os.Create(path.Join("/var/lib/registry", "/docker/registry/v2/mv_tmp_servertars", server, tmp_dir)) // added a tmp_dir
				if err != nil{
					context.GetLogger(ctx).Errorf("NANNAN: PrepareCopy <COMPRESS create file> %s, ", err)
					errChan <- err
				}else{
				
					defer packFile.Close()
					
					_, err := io.Copy(packFile, data)
					if err != nil{
						context.GetLogger(ctx).Errorf("NANNAN: Copy compress file <COMPRESS copy to desfile> %s, ", err)
						errChan <- err
					}else{
						tarpathChan <- packFile.Name()
					}
				}
			}
			limChan <- true 
		}(server)
	}
	
	mvtarpaths := []string{}
	for{
		select{
			case err := <-errChan:
				return []string{}, err
			case res := <-tarpathChan:
				mvtarpaths = append(mvtarpaths, res)
		}
		
		if len(serverForwardMap) == len(mvtarpaths){
			break
		}
	}
	return mvtarpaths, nil
}

//NANNAN: after finishing commit, start do deduplication
//TODO delete tarball
//type BFmap map[digest.Digest][]distribution.FileDescriptor

/*
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/
1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/
1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/diff/
NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/
1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/diff/
8b6566f585bad55b6fb9efb1dc1b6532fd08bb1796b4b42a3050aacb961f1f3f
*/

func (bw *blobWriter) Dedup(ctx context.Context, desc distribution.Descriptor) (error) {
	
	context.GetLogger(ctx).Debug("NANNAN: (*blobWriter).Dedup")

	blobPath, err := PathFor(BlobDataPathSpec{
		Digest: desc.Digest,
	})
	context.GetLogger(ctx).Debugf("NANNAN: blob = %v:%v", blobPath, desc.Digest)
	
	_, err = bw.blobStore.registry.fileDescriptorCacheProvider.StatBFRecipe(ctx, desc.Digest)
	if err == nil{
		context.GetLogger(ctx).Debug("NANNAN: THIS LAYER TARBALL ALREADY DEDUPED :=>%v", desc.Digest)
		return nil
	}
	
	//DedupLayersFromPath(blobPath)
	//log.Warnf("IBM: HTTP GET: %s", dgst)
	//WithField("digest", desc.Digest).Warnf("attempted to move zero-length content with non-zero digest")
	
	layerPath := path.Join("/var/lib/registry", blobPath)
	
	context.GetLogger(ctx).Debug("NANNAN: START DEDUPLICATION FROM PATH :=>%s", layerPath)

	parentDir := path.Dir(layerPath)
	unpackPath := path.Join(parentDir, "diff")

	archiver := archive.NewDefaultArchiver()
	options := &archive.TarOptions{
		UIDMaps: archiver.IDMapping.UIDs(),
		GIDMaps: archiver.IDMapping.GIDs(),
	}
	idMapping := idtools.NewIDMappingsFromMaps(options.UIDMaps, options.GIDMaps)
	rootIDs := idMapping.RootPair()
	err = idtools.MkdirAllAndChownNew(unpackPath, 0777, rootIDs)
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s", err)
		return err
	}

	err = archiver.UntarPath(layerPath, unpackPath)
	if err != nil {
		//TODO: process manifest file
		context.GetLogger(ctx).Errorf("NANNAN: %s, This may be a manifest file", err)
		return err
	}
	
	gid := getGID()
	//later check ..................
	// check if we need to dedup this tarball
	files, err := ioutil.ReadDir(unpackPath)
	if err != nil{
		context.GetLogger(ctx).Errorf("NANNAN: %s, cannot read this tar file", err)
	}
	for _, f := range files{
		fmatch, _ := path.Match("NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL", f.Name())
		if fmatch{
			context.GetLogger(ctx).Debug("NANNAN: NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL ", f.Name())
			/*
			 /home/nannan/dockerimages/layers
			 /docker/registry/v2/blobs/sha256/07/078bb24d9ee4ddf90f349d0b63004d3ac6897dae28dd37cc8ae97a0306e6aa33/
			 diff/NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/1b/1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced/diff	
			*/
			//move unpackPath to the correct diff dir
			//newpathname := os.Rename(path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/"), "/var/lib/registry/docker/registry/v2/")
			
			files, err := ioutil.ReadDir(path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/"))
			if err != nil {
			    context.GetLogger(ctx).Errorf("NANNAN: %s, cannot read this unpackpath file: %s", path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/"), err)
			    return err
			}
			for _, f := range files {
				context.GetLogger(ctx).Debug("NANNAN: find a layer subdir xx: ", f.Name())
				if _, err := os.Stat(path.Join("/var/lib/registry/docker/registry/v2/blobs/sha256/", f.Name())); err == nil{
					//path exists
					//get next level directories
					fds, err := ioutil.ReadDir(path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name()))
					if err != nil{
						context.GetLogger(ctx).Errorf("NANNAN: %s, cannot read this unpackpath filepath: %s", path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name()), err)
						return err
					}
					for _, fd := range fds{
						if err = os.Rename(path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name(), fd.Name()), 
							path.Join("/var/lib/registry/docker/registry/v2/blobs/sha256/", f.Name(), fd.Name())); err != nil{
							context.GetLogger(ctx).Errorf("NANNAN: %s, cannot rename this unpackpath filepath: %s", 
								path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name(),fd.Name()), err)
							return err
						}
					}
					
				}else{
					    if err = os.Rename(path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name()), 
							path.Join("/var/lib/registry/docker/registry/v2/blobs/sha256/", f.Name())); err != nil{
							context.GetLogger(ctx).Errorf("NANNAN: %s, cannot rename this unpackpath filepath: %s", 
								path.Join(unpackPath, "NANNAN_NO_NEED_TO_DEDUP_THIS_TARBALL/docker/registry/v2/blobs/sha256/", f.Name()), err)
							return err
						}
				}
			}
			return nil
		}
	}
	
	var bfdescriptors [] distribution.BFDescriptor
	var serverIps []string
	serverForwardMap := make(map[string][]string)
	
	err = filepath.Walk(unpackPath, bw.CheckDuplicate(ctx, bw.blobStore.registry.serverIp, desc, bw.blobStore.registry.fileDescriptorCacheProvider, 
			&bfdescriptors, 
			&serverIps,
			serverForwardMap))
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s", err)
	}
	
	serverIps = append(serverIps, bw.blobStore.registry.serverIp) //NANNAN add this serverip
	
	des := distribution.BFRecipeDescriptor{
		BlobDigest: desc.Digest,
		BFDescriptors: bfdescriptors,
		ServerIps: RemoveDuplicateIpsFromIps(serverIps),
	}
//	context.GetLogger(ctx).Debug("NANNAN: set distribution.BFRecipeDescriptor: %v", des)
	err = bw.blobStore.registry.fileDescriptorCacheProvider.SetBFRecipe(ctx, desc.Digest, des)
	if err != nil {
		return err
	}
	// let's do forwarding
	var wg sync.WaitGroup
	mvtarpaths, err := bw.PrepareForward(ctx, serverForwardMap, gid)
	if err != nil{
		return err
	}
	
	if len(mvtarpaths) == 0{
		return nil
	}
	
	context.GetLogger(ctx).Debug("NANNAN: mvtarpaths are %v", mvtarpaths)
//	ch := make(chan error, len(mvtarpaths))
	for _, path := range mvtarpaths{
		wg.Add(1)
		go bw.ForwardToRegistry(ctx, path, &wg) //ForwardToRegistry(ctx context.Context, regname string, path string)
		go func() {
	        wg.Wait()
//	        close(sizes)
	    }()
	}
	
	return err
}

/*
NANNAN check dedup
 Metrics: lock
 
*/

func (bw *blobWriter) CheckDuplicate(ctx context.Context, serverIp string, desc distribution.Descriptor, db cache.FileDescriptorCacheProvider, 
	bfdescriptors *[] distribution.BFDescriptor, 
	serverIps *[] string,
	serverForwardMap map[string][]string) filepath.WalkFunc {
//	totalFiles := 0
//	sameFiles := 0
//	reguFiles := 0
//	rmFiles := 0
	
	return func(fpath string, info os.FileInfo, err error) error {
//		context.GetLogger(ctx).Debug("NANNAN: START CHECK DUPLICATES :=>")

		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: ", err)
			return err
		}
//		context.GetLogger(ctx).Debug("NANNAN: totalFiles,sameFiles,reguFiles,rmFiles", totalFiles, sameFiles, reguFiles, rmFiles)
//		if info.IsDir() {
//			context.GetLogger(ctx).Debug("NANNAN: TODO process directories")
//			
//			return nil
//		}
		
		//NANNAN: CHECK file stat, skip symlink and hardlink
//		totalFiles = totalFiles + 1
		
		if ! (info.Mode().IsRegular()){
//			context.GetLogger(ctx).Debug("NANNAN: TODO process sysmlink and othrs")
			return nil
		}
		
//		reguFiles = reguFiles + 1
			
		fp, err := os.Open(fpath) 
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: %s", err)
			return nil
		}
		
		defer fp.Close()
		
		digestFn := algorithm.FromReader
		dgst, err := digestFn(fp)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: %s: %v", fpath, err)
			return err
		}
		
		//NANNAN: add file to map	

		// var serverIps []string
		des, err := db.StatFile(ctx, dgst)
		if err == nil {
			// file content already present	
			//first update layer metadata
			//delete this file
//			sameFiles = sameFiles + 1
			//if fpath == des.FilePath{
			//	context.GetLogger(ctx).Debug("NANNAN: This layer tarball has already deduped: %v!\n", dgst)
			//	return nil
			//}
			err := os.Remove(fpath)
			if err != nil {
			  context.GetLogger(ctx).Errorf("NANNAN: %s", err)
			  return err
			}
//			rmFiles = rmFiles + 1
//			context.GetLogger(ctx).Debug("NANNAN: REMVE file %s", path)
			
			dfp := des.FilePath
			bfdescriptor := distribution.BFDescriptor{
				BlobFilePath: fpath,
				Digest:    dgst,
				DigestFilePath: dfp,
				ServerIp:	des.ServerIp,
			}
			
			*bfdescriptors = append(*bfdescriptors, bfdescriptor)
			*serverIps = append(*serverIps, des.ServerIp)
			
			return nil
		} else if err != redisgo.Nil {
			context.GetLogger(ctx).Errorf("NANNAN: checkDuplicate: error stating content (%v): %v", dgst, err)
			// real error, return it
//			fmt.Println(err)
			return err
			//return distribution.Descriptor{}, err	
		}
		
		//to avoid invalid filepath, rename the original file to digest //tarfpath := strings.SplitN(dgst.String(), ":", 2)[1]
		
		reFPath := path.Join(path.Dir(fpath), strings.SplitN(dgst.String(), ":", 2)[1])
		err = os.Rename(fpath, reFPath)
		if err != nil{
			context.GetLogger(ctx).Errorf("NANNAN: fail to rename path (%v): %v", fpath, reFPath)
			return err
		}
		
		fpath = reFPath
		//serverForwardMap := make(map[string][]string)
		server, _:= bw.blobStore.registry.blobServer.ring.GetNode(dgst.String())
		context.GetLogger(ctx).Debug("NANNAN: file: %v (%v) will be forwarded to server (%v): %v", dgst.String(), reFPath, server)
	//	var desc distribution.FileDescriptor	
		//make map of []
		// need to forward to other servers
		if server != serverIp{ 
			serverForwardMap[server] = append(serverForwardMap[server], fpath)
		}
	
		des = distribution.FileDescriptor{
			
	//		Size: int64(len(p)),
			// NOTE(stevvooe): The central blob store firewalls media types from
			// other users. The caller should look this up and override the value
			// for the specific repository.
			FilePath: fpath,
			Digest:    dgst,
			ServerIp: server,//serverIp,
		}
		
		err = db.SetFileDescriptor(ctx, dgst, des)
		if err != nil {
			return err
		}
		//NANNAN: Here, we distributed all the files to different servers.
		
		dfp := des.FilePath
		bfdescriptor := distribution.BFDescriptor{
			BlobFilePath: fpath,
			Digest:    dgst,
			DigestFilePath: dfp,
			ServerIp: server,//serverIp,
		}
				
		*bfdescriptors = append(*bfdescriptors, bfdescriptor)
		
		return nil
	}
}


// Cancel the blob upload process, releasing any resources associated with
// the writer and canceling the operation.
func (bw *blobWriter) Cancel(ctx context.Context) error {
	context.GetLogger(ctx).Debug("(*blobWriter).Cancel")
	if err := bw.fileWriter.Cancel(); err != nil {
		return err
	}

	if err := bw.Close(); err != nil {
		context.GetLogger(ctx).Errorf("error closing blobwriter: %s", err)
	}

	if err := bw.removeResources(ctx); err != nil {
		return err
	}

	return nil
}

func (bw *blobWriter) Size() int64 {
	return bw.fileWriter.Size()
}

func (bw *blobWriter) Write(p []byte) (int, error) {
	// Ensure that the current write offset matches how many bytes have been
	// written to the digester. If not, we need to update the digest state to
	// match the current write position.
	if err := bw.resumeDigest(bw.blobStore.ctx); err != nil && err != errResumableDigestNotAvailable {
		return 0, err
	}

	n, err := io.MultiWriter(bw.fileWriter, bw.digester.Hash()).Write(p)
	bw.written += int64(n)

	return n, err
}

func (bw *blobWriter) ReadFrom(r io.Reader) (n int64, err error) {
	// Ensure that the current write offset matches how many bytes have been
	// written to the digester. If not, we need to update the digest state to
	// match the current write position.
	if err := bw.resumeDigest(bw.blobStore.ctx); err != nil && err != errResumableDigestNotAvailable {
		return 0, err
	}

	nn, err := io.Copy(io.MultiWriter(bw.fileWriter, bw.digester.Hash()), r)
	bw.written += nn

	return nn, err
}

func (bw *blobWriter) Close() error {
	if bw.committed {
		return errors.New("blobwriter close after commit")
	}

	if err := bw.storeHashState(bw.blobStore.ctx); err != nil && err != errResumableDigestNotAvailable {
		return err
	}

	return bw.fileWriter.Close()
}

// validateBlob checks the data against the digest, returning an error if it
// does not match. The canonical descriptor is returned.
func (bw *blobWriter) validateBlob(ctx context.Context, desc distribution.Descriptor) (distribution.Descriptor, error) {
	var (
		verified, fullHash bool
		canonical          digest.Digest
	)

	if desc.Digest == "" {
		// if no descriptors are provided, we have nothing to validate
		// against. We don't really want to support this for the registry.
		return distribution.Descriptor{}, distribution.ErrBlobInvalidDigest{
			Reason: fmt.Errorf("cannot validate against empty digest"),
		}
	}

	var size int64

	// Stat the on disk file
	if fi, err := bw.driver.Stat(ctx, bw.path); err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			// NOTE(stevvooe): We really don't care if the file is
			// not actually present for the reader. We now assume
			// that the desc length is zero.
			desc.Size = 0
		default:
			// Any other error we want propagated up the stack.
			return distribution.Descriptor{}, err
		}
	} else {
		if fi.IsDir() {
			return distribution.Descriptor{}, fmt.Errorf("unexpected directory at upload location %q", bw.path)
		}

		size = fi.Size()
	}

	if desc.Size > 0 {
		if desc.Size != size {
			return distribution.Descriptor{}, distribution.ErrBlobInvalidLength
		}
	} else {
		// if provided 0 or negative length, we can assume caller doesn't know or
		// care about length.
		desc.Size = size
	}

	// TODO(stevvooe): This section is very meandering. Need to be broken down
	// to be a lot more clear.

	if err := bw.resumeDigest(ctx); err == nil {
		canonical = bw.digester.Digest()

		if canonical.Algorithm() == desc.Digest.Algorithm() {
			// Common case: client and server prefer the same canonical digest
			// algorithm - currently SHA256.
			verified = desc.Digest == canonical
		} else {
			// The client wants to use a different digest algorithm. They'll just
			// have to be patient and wait for us to download and re-hash the
			// uploaded content using that digest algorithm.
			fullHash = true
		}
	} else if err == errResumableDigestNotAvailable {
		// Not using resumable digests, so we need to hash the entire layer.
		fullHash = true
	} else {
		return distribution.Descriptor{}, err
	}

	if fullHash {
		// a fantastic optimization: if the the written data and the size are
		// the same, we don't need to read the data from the backend. This is
		// because we've written the entire file in the lifecycle of the
		// current instance.
		if bw.written == size && digest.Canonical == desc.Digest.Algorithm() {
			canonical = bw.digester.Digest()
			verified = desc.Digest == canonical
		}

		// If the check based on size fails, we fall back to the slowest of
		// paths. We may be able to make the size-based check a stronger
		// guarantee, so this may be defensive.
		if !verified {
			digester := digest.Canonical.Digester()
			verifier := desc.Digest.Verifier()

			// Read the file from the backend driver and validate it.
			fr, err := newFileReader(ctx, bw.driver, bw.path, desc.Size)
			if err != nil {
				return distribution.Descriptor{}, err
			}
			defer fr.Close()

			tr := io.TeeReader(fr, digester.Hash())

			if _, err := io.Copy(verifier, tr); err != nil {
				return distribution.Descriptor{}, err
			}

			canonical = digester.Digest()
			verified = verifier.Verified()
		}
	}

	if !verified {
		context.GetLoggerWithFields(ctx,
			map[interface{}]interface{}{
				"canonical": canonical,
				"provided":  desc.Digest,
			}, "canonical", "provided").
			Errorf("canonical digest does match provided digest")
		return distribution.Descriptor{}, distribution.ErrBlobInvalidDigest{
			Digest: desc.Digest,
			Reason: fmt.Errorf("content does not match digest"),
		}
	}

	// update desc with canonical hash
	desc.Digest = canonical

	if desc.MediaType == "" {
		desc.MediaType = "application/octet-stream"
	}

	return desc, nil
}

// moveBlob moves the data into its final, hash-qualified destination,
// identified by dgst. The layer should be validated before commencing the
// move.
func (bw *blobWriter) moveBlob(ctx context.Context, desc distribution.Descriptor) error {
	blobPath, err := pathFor(blobDataPathSpec{
		digest: desc.Digest,
	})

	if err != nil {
		return err
	}

	// Check for existence
	if _, err := bw.blobStore.driver.Stat(ctx, blobPath); err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			break // ensure that it doesn't exist.
		default:
			return err
		}
	} else {
		// If the path exists, we can assume that the content has already
		// been uploaded, since the blob storage is content-addressable.
		// While it may be corrupted, detection of such corruption belongs
		// elsewhere.
		return nil
	}

	// If no data was received, we may not actually have a file on disk. Check
	// the size here and write a zero-length file to blobPath if this is the
	// case. For the most part, this should only ever happen with zero-length
	// blobs.
	if _, err := bw.blobStore.driver.Stat(ctx, bw.path); err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			// HACK(stevvooe): This is slightly dangerous: if we verify above,
			// get a hash, then the underlying file is deleted, we risk moving
			// a zero-length blob into a nonzero-length blob location. To
			// prevent this horrid thing, we employ the hack of only allowing
			// to this happen for the digest of an empty blob.
			if desc.Digest == digestSha256Empty {
				return bw.blobStore.driver.PutContent(ctx, blobPath, []byte{})
			}

			// We let this fail during the move below.
			logrus.
				WithField("upload.id", bw.ID()).
				WithField("digest", desc.Digest).Warnf("attempted to move zero-length content with non-zero digest")
		default:
			return err // unrelated error
		}
	}

	// TODO(stevvooe): We should also write the mediatype when executing this move.

	return bw.blobStore.driver.Move(ctx, bw.path, blobPath)
}

// removeResources should clean up all resources associated with the upload
// instance. An error will be returned if the clean up cannot proceed. If the
// resources are already not present, no error will be returned.
func (bw *blobWriter) removeResources(ctx context.Context) error {
	dataPath, err := pathFor(uploadDataPathSpec{
		name: bw.blobStore.repository.Named().Name(),
		id:   bw.id,
	})

	if err != nil {
		return err
	}

	// Resolve and delete the containing directory, which should include any
	// upload related files.
	dirPath := path.Dir(dataPath)
	if err := bw.blobStore.driver.Delete(ctx, dirPath); err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			break // already gone!
		default:
			// This should be uncommon enough such that returning an error
			// should be okay. At this point, the upload should be mostly
			// complete, but perhaps the backend became unaccessible.
			context.GetLogger(ctx).Errorf("unable to delete layer upload resources %q: %v", dirPath, err)
			return err
		}
	}

	return nil
}

func (bw *blobWriter) Reader() (io.ReadCloser, error) {
	// todo(richardscothern): Change to exponential backoff, i=0.5, e=2, n=4
	try := 1
	for try <= 5 {
		_, err := bw.driver.Stat(bw.ctx, bw.path)
		if err == nil {
			break
		}
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			context.GetLogger(bw.ctx).Debugf("Nothing found on try %d, sleeping...", try)
			time.Sleep(1 * time.Second)
			try++
		default:
			return nil, err
		}
	}

	readCloser, err := bw.driver.Reader(bw.ctx, bw.path, 0)
	if err != nil {
		return nil, err
	}

	return readCloser, nil
}
