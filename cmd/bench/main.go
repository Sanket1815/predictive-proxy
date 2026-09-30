// Command bench measures Range-read latency for an education-portal workload
// (lecture videos, PDFs, slide decks, dataset downloads) against S3 directly
// and through the predictive proxy.
//
//	bench check -bucket B -region R    verify credentials, bucket access and dataset size
//	bench seed  -bucket B -region R    upload the portal dataset (synthetic course files
//	                                   and/or your own real files via -include-dir)
//	bench run   -bucket B -region R    replay student sessions and write results
//
// Credentials come from the AWS SDK default chain (env vars, ~/.aws/credentials,
// SSO, instance role); nothing secret is passed on the command line.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "check":
		err = runCheck(os.Args[2:])
	case "seed":
		err = runSeed(os.Args[2:])
	case "run":
		err = runBench(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench <check|seed|run> [flags]   (bench <cmd> -h for flags)")
	os.Exit(2)
}
