module github.com/sequinstream/sequin-go/examples/simple

go 1.19

require (
	github.com/sequinstream/sequin-go v0.0.0
	golang.org/x/sync v0.8.0 // indirect
)

// Point to the local package relative to this module
replace github.com/sequinstream/sequin-go => ../../
