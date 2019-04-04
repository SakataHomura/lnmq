package qutils

import (
    "math/rand"
)

func UniqRands(quantity int, max int) []int {
    if max < quantity {
        max = quantity
    }

    ret := make([]int, max)
    for i:=0; i<max; i++  {
        ret[i] = i
    }

    for i:=0; i<quantity; i++  {
        j := rand.Int() % max + i
        ret[i], ret[j] = ret[j], ret[i]
        max --
    }

    return ret[0:quantity]
}