# Table

一个没什么用的 table

```go
package main

import (
	"fmt"
	"github.com/equationzhao/table"
)

func main() {
	table := table.NewTable(8)

	table.Insert("key1", 1)
	table.Insert("key2", 2)

	value := table.Find("key1")
	fmt.Println("key1:", value)

	table.Expand(16)

	table.Shrink()
}
```