package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
)

const UnixfsChunkSize uint64 = 1 << 20
const UnixfsLinksPerLevel = 1024

func ClientImport(ctx context.Context, path string) (cid.Cid, error) {
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bsvc := blockservice.New(bstore, offline.Exchange(bstore))
	localDAG := merkledag.NewDAGService(bsvc)

	f, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	stat, err := f.Stat()
	if err != nil {
		return cid.Undef, err
	}

	file, err := files.NewReaderPathFile(path, f, stat)
	if err != nil {
		return cid.Undef, err
	}

	bufferedDS := ipld.NewBufferedDAG(ctx, localDAG)

	params := ihelper.DagBuilderParams{
		Maxlinks:   UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
		NoCopy:     true,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(UnixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	if err := bufferedDS.Commit(); err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}
func TestClientImport(t *testing.T) {
	ctx := context.Background()
	cid, err := ClientImport(ctx, "/tmp/data")
	if err != nil {
		t.Error(err)
	}
	fmt.Println("cid:", cid)
}

func ClientImportLocal(ctx context.Context, f io.Reader) (cid.Cid, error) {
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bsvc := blockservice.New(bstore, offline.Exchange(bstore))

	localDAG := merkledag.NewDAGService(bsvc)

	file := files.NewReaderFile(f)

	bufferedDS := ipld.NewBufferedDAG(ctx, localDAG)

	params := ihelper.DagBuilderParams{
		Maxlinks:   UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(UnixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), bufferedDS.Commit()
}
func TestClientImportLocal(t *testing.T) {
	http.HandleFunc("/import", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		cid, err := ClientImportLocal(ctx, r.Body)
		if err != nil {
			t.Error(err)
		}
		fmt.Println("cid:", cid)
		fmt.Fprintf(w, "cid: %v\n", cid)
	})
	fmt.Println("Jim1")
	srv := &http.Server{
		Addr:    ":8080",
		Handler: http.DefaultServeMux,
	}
	go func() {
		client := &http.Client{}
		url := "http://localhost:8080/import"
		f, err := os.Open("/tmp/data")
		if err != nil {
			t.Error(err)
		}
		r := bufio.NewReader(f)
		req, err := http.NewRequest("PUT", url, r)
		if err != nil {
			t.Error(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Error(err)
		}
		fmt.Println("Body:", string(body))
		// srv.Close()
	}()

	log.Fatal(srv.ListenAndServe())
}
