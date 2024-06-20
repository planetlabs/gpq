package pqutil

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/parquet"
	pqschema "github.com/apache/arrow/go/v16/parquet/schema"
)

var ParquetStringType = pqschema.StringLogicalType{}

func LookupNode(schema *pqschema.Schema, name string) (pqschema.Node, bool) {
	root := schema.Root()
	index := root.FieldIndexByName(name)
	if index < 0 {
		return nil, false
	}

	return root.Field(index), true
}

func LookupPrimitiveNode(schema *pqschema.Schema, name string) (*pqschema.PrimitiveNode, bool) {
	node, ok := LookupNode(schema, name)
	if !ok {
		return nil, false
	}

	primitive, ok := node.(*pqschema.PrimitiveNode)
	return primitive, ok
}

func LookupGroupNode(schema *pqschema.Schema, name string) (*pqschema.GroupNode, bool) {
	node, ok := LookupNode(schema, name)
	if !ok {
		return nil, false
	}

	group, ok := node.(*pqschema.GroupNode)
	return group, ok
}

func LookupListElementNode(sc *pqschema.Schema, name string) (*pqschema.PrimitiveNode, bool) {
	node, ok := LookupGroupNode(sc, name)
	if !ok {
		return nil, false
	}

	if node.NumFields() != 1 {
		return nil, false
	}

	group, ok := node.Field(0).(*pqschema.GroupNode)
	if !ok {
		return nil, false
	}

	if group.NumFields() != 1 {
		return nil, false
	}

	element, ok := group.Field(0).(*pqschema.PrimitiveNode)
	return element, ok
}

// ParquetSchemaString generates a string representation of the schema as documented
// in https://pkg.go.dev/github.com/fraugster/parquet-go/parquetschema
func ParquetSchemaString(schema *pqschema.Schema) string {
	w := &parquetWriter{}
	return w.String(schema)
}

type parquetWriter struct {
	builder *strings.Builder
	err     error
}

func (w *parquetWriter) String(schema *pqschema.Schema) string {
	w.builder = &strings.Builder{}
	w.err = nil
	w.writeSchema(schema)
	if w.err != nil {
		return w.err.Error()
	}
	return w.builder.String()
}

func (w *parquetWriter) writeLine(str string, level int) {
	if w.err != nil {
		return
	}
	indent := strings.Repeat("  ", level)
	if _, err := w.builder.WriteString(indent + str + "\n"); err != nil {
		w.err = err
	}
}

func (w *parquetWriter) writeSchema(schema *pqschema.Schema) {
	w.writeLine("message {", 0)
	root := schema.Root()
	for i := 0; i < root.NumFields(); i += 1 {
		w.writeNode(root.Field(i), 1)
	}
	w.writeLine("}", 0)
}

func (w *parquetWriter) writeNode(node pqschema.Node, level int) {
	switch n := node.(type) {
	case *pqschema.GroupNode:
		w.writeGroupNode(n, level)
	case *pqschema.PrimitiveNode:
		w.writePrimitiveNode(n, level)
	default:
		w.writeLine(fmt.Sprintf("unknown node type: %v", node), level)
	}
}

func (w *parquetWriter) writeGroupNode(node *pqschema.GroupNode, level int) {
	repetition := node.RepetitionType().String()
	name := node.Name()
	annotation := LogicalOrConvertedAnnotation(node)

	w.writeLine(fmt.Sprintf("%s group %s%s {", repetition, name, annotation), level)
	for i := 0; i < node.NumFields(); i += 1 {
		w.writeNode(node.Field(i), level+1)
	}
	w.writeLine("}", level)
}

func (w *parquetWriter) writePrimitiveNode(node *pqschema.PrimitiveNode, level int) {
	repetition := node.RepetitionType().String()
	name := node.Name()
	nodeType := physicalTypeString(node.PhysicalType())
	annotation := LogicalOrConvertedAnnotation(node)

	w.writeLine(fmt.Sprintf("%s %s %s%s;", repetition, nodeType, name, annotation), level)
}

func LogicalOrConvertedAnnotation(node pqschema.Node) string {
	logicalType := node.LogicalType()
	convertedType := node.ConvertedType()

	switch t := logicalType.(type) {
	case *pqschema.IntLogicalType:
		return fmt.Sprintf(" (INT (%d, %t))", t.BitWidth(), t.IsSigned())
	case *pqschema.DecimalLogicalType:
		return fmt.Sprintf(" (DECIMAL (%d, %d))", t.Precision(), t.Scale())
	case *pqschema.TimestampLogicalType:
		var unit string
		switch t.TimeUnit() {
		case pqschema.TimeUnitMillis:
			unit = "MILLIS"
		case pqschema.TimeUnitMicros:
			unit = "MICROS"
		case pqschema.TimeUnitNanos:
			unit = "NANOS"
		default:
			unit = "UNKNOWN"
		}
		return fmt.Sprintf(" (TIMESTAMP (%s, %t))", unit, t.IsAdjustedToUTC())
	}

	var annotation string
	_, invalid := logicalType.(pqschema.UnknownLogicalType)
	_, none := logicalType.(pqschema.NoLogicalType)

	if logicalType != nil && !invalid && !none {
		annotation = fmt.Sprintf(" (%s)", strings.ToUpper(logicalType.String()))
	} else if convertedType != pqschema.ConvertedTypes.None {
		annotation = fmt.Sprintf(" (%s)", strings.ToUpper(convertedType.String()))
	}

	return annotation
}

var physicalTypeLookup = map[string]string{
	"byte_array": "binary",
}

func physicalTypeString(physical parquet.Type) string {
	nodeType := strings.ToLower(physical.String())
	if altType, ok := physicalTypeLookup[nodeType]; ok {
		return altType
	}
	if physical == parquet.Types.FixedLenByteArray {
		nodeType += fmt.Sprintf(" (%d)", physical.ByteSize())
	}
	return nodeType
}
