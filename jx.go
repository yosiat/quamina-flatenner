package main

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/go-faster/jx"
	"github.com/timbray/quamina"
)

type jxFlattener struct {
	paths PathIndex

	fields     []quamina.Field
	dcd        *jx.Decoder
	arrayCount int32
	arrayTrail []quamina.ArrayPos
}

func newJxFlattener(paths PathIndex) *jxFlattener {
	return &jxFlattener{
		paths:      paths,
		fields:     make([]quamina.Field, 0),
		arrayTrail: make([]quamina.ArrayPos, 0),
		arrayCount: 0,
	}
}

func (fj *jxFlattener) Copy() quamina.Flattener {
	return &jxFlattener{
		paths:      fj.paths,
		fields:     make([]quamina.Field, 0),
		arrayTrail: make([]quamina.ArrayPos, 0),
		arrayCount: 0,
	}
}

func (fj *jxFlattener) reset() {
	fj.arrayCount = 0
	fj.fields = fj.fields[:0]
	fj.arrayTrail = fj.arrayTrail[:0]
}

func (fj *jxFlattener) getDecoder(event []byte) *jx.Decoder {
	dcd := jx.GetDecoder()
	dcd.ResetBytes(event)

	return dcd
}

func (fj *jxFlattener) Flatten(event []byte, tracker quamina.NameTracker) ([]quamina.Field, error) {
	fj.reset()

	//fmt.Println()
	//fmt.Printf("Paths: %+v\n", fj.paths)
	//fmt.Printf("Input: %s\n\n", string(event))

	// Setup a decoder.
	fj.dcd = fj.getDecoder(event)
	defer jx.PutDecoder(fj.dcd)

	if err := fj.traverseNode(fj.paths); err != nil {
		return fj.fields, err
	}

	//fmt.Printf("Finished flatenning\n")
	//fmt.Println()

	return fj.fields, nil
}

// Traverse a node - all nodes are treated as objects.
//
//	Goes into it and find all sub-nodes and eventually all the fields.
func (fj *jxFlattener) traverseNode(n Node) error {
	nodeFields := n.getFields()
	fieldsCount := len(nodeFields)

	// Get count of how many nodes we have in this level.
	// Once we get to count of zero, we know we can stop parsing.
	nodesCount := n.nodesCount()
	//fmt.Printf("Nodes: \"%s\" (count: %d), fields count: %d\n", strings.Join(n.names(), ", "), nodesCount, fieldsCount)

	objIter, err := fj.dcd.ObjIter()
	if err != nil {
		return fmt.Errorf("failed traversing node: %s", err)
	}

	for objIter.Next() {
		key := BinaryString(objIter.Key())
		//fmt.Printf("[%s] entering\n", key)

		// If the type of the current property is object
		// let's check if it's a node, otherwise we are going to skip this property.
		if fj.dcd.Next() == jx.Object {
			if node, found := n.get(key); found {
				if err := fj.traverseNode(node); err != nil {
					return err
				}

				nodesCount--
				//fmt.Printf("\t[%s] Reducing nodes count, current is %d\n", key, nodesCount)

				if fieldsCount == 0 && nodesCount == 0 {
					//fmt.Printf("\t[%s] nodes > breaking.\n", key)
					break
				} else {
					continue
				}
			}
		} else {
			path, isField := nodeFields[key]
			if isField {
				//fmt.Printf("\t[%s] is a field.\n", key)
				if err := fj.parseField(path, n); err != nil {
					return err
				}

				fieldsCount--

				// We can't break here, since we might have to return
				// to parent node, so we will need to parse more fields.
				// I can come up with a way but I think it will be complex.
				continue
			}
		}

		if err := fj.dcd.Skip(); err != nil {
			return fmt.Errorf("traverseNode: failed skipping: %s", err)
		}
		//fmt.Printf("\t[%s] finished\n", key)
	}
	//fmt.Printf("Finished Processing")

	return nil
}

func (fj *jxFlattener) parseField(path []byte, n Node) error {
	typ := fj.dcd.Next()

	if typ == jx.Array {
		//fmt.Printf("\t\tparseField: %s\n", strings.ReplaceAll(string(path), "\n", "->"))
		return fj.parseArrayField(path, n)
	}

	if typ == jx.String || typ == jx.Number || typ == jx.Bool || typ == jx.Null {
		return fj.parsePrimitiveField(path, n)
	}

	return fmt.Errorf("parseField: don't know how to handle: %s", typ)
}

func (fj *jxFlattener) parsePrimitiveField(path []byte, n Node) error {
	f := quamina.Field{}

	val, err := fj.getPrimitiveValue()
	if err != nil {
		return err
	}

	f.Val = val
	f.Path = path
	fj.fields = append(fj.fields, f)

	return nil

}

func (fj *jxFlattener) getPrimitiveValue() (val []byte, err error) {
	// We wil use "Raw" value, since we want to return in the end byte array.
	// It's important to note that "Raw" will return the value as is,
	//   so for strings it will return them with quotes,
	//   numbers in array it will returen them with spaces if there are any.
	val, err = fj.dcd.Raw()
	if err != nil {
		return
	}

	return bytes.Trim(val, " "), err
}

func (fj *jxFlattener) parseArrayField(path []byte, n Node) error {
	iter, err := fj.dcd.ArrIter()
	if err != nil {
		return err
	}

	fj.enterArray()
	defer fj.leaveArray()

	for iter.Next() {
		fj.stepOneArrayElement()

		typ := fj.dcd.Next()

		if typ == jx.Array {
			// If value is an array, enter it.
			if err := fj.parseArrayField(path, n); err != nil {
				return err
			}
		}

		if typ == jx.String || typ == jx.Number || typ == jx.Bool || typ == jx.Null {
			// If it's primtive value append to the list.
			val, err := fj.getPrimitiveValue()
			if err != nil {
				return err
			}

			fj.storeArrayElementField(path, val)
		}
	}

	return nil

}

func (fj *jxFlattener) storeArrayElementField(path []byte, val []byte) {
	f := quamina.Field{Path: path, ArrayTrail: make([]quamina.ArrayPos, len(fj.arrayTrail)), Val: val}
	copy(f.ArrayTrail, fj.arrayTrail)
	fj.fields = append(fj.fields, f)
}

func (fj *jxFlattener) enterArray() {
	fj.arrayCount++
	fj.arrayTrail = append(fj.arrayTrail, quamina.ArrayPos{Array: fj.arrayCount, Pos: 0})
}

func (fj *jxFlattener) leaveArray() {
	fj.arrayTrail = fj.arrayTrail[:len(fj.arrayTrail)-1]
}

func (fj *jxFlattener) stepOneArrayElement() {
	fj.arrayTrail[len(fj.arrayTrail)-1].Pos++
}

// Source: https://github.com/rueian/rueidis/blob/master/binary.go#L13
//
// BinaryString convert the provided []byte into a string without copy. It does what strings.Builder.String() does.
// Redis Strings are binary safe, this means that it is safe to store any []byte into Redis directly.
// Users can use this BinaryString helper to insert a []byte as the part of redis command. For example:client.B().Set().Key(rueidis.BinaryString([]byte{0})).Value(rueidis.BinaryString([]byte{0})).Build()
//
// To read back the []byte of the string returned from the Redis, it is recommended to use the RedisMessage.AsReader.
func BinaryString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
