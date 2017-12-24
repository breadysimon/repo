package main

import (
	"fmt"

	"os/exec"
)

func main() {
	err := exec.Command("cmd.exe", "/c", "start kubectl exec -it wdcloud-payment-node-1969379705-vx67l sh").Run()

	if err != nil {
		fmt.Print(err)
	}

}
