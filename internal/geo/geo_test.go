package geo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBboxIntersectsTrue(t *testing.T) {
	box1 := &Bbox{
		Xmin: 10,
		Ymin: 20,
		Xmax: 30,
		Ymax: 40,
	}

	box2 := &Bbox{
		Xmin: 25,
		Ymin: 35,
		Xmax: 45,
		Ymax: 55,
	}

	require.Equal(t, box1.Intersects(box2), true)
}

func TestBboxIntersectsFalse(t *testing.T) {
	box1 := &Bbox{
		Xmin: -10,
		Ymin: 20,
		Xmax: -5,
		Ymax: 40,
	}

	box2 := &Bbox{
		Xmin: -1,
		Ymin: 50,
		Xmax: 0,
		Ymax: 70,
	}

	require.Equal(t, box1.Intersects(box2), false)
}

func TestBboxIntersectsTouches(t *testing.T) {
	box1 := &Bbox{
		Xmin: 10,
		Ymin: 20,
		Xmax: 30,
		Ymax: 40,
	}

	box2 := &Bbox{
		Xmin: 30,
		Ymin: 20,
		Xmax: 40,
		Ymax: 40,
	}

	require.Equal(t, box1.Intersects(box2), true)
}

func TestBboxIntersectsContains(t *testing.T) {
	box1 := &Bbox{
		Xmin: 10,
		Ymin: 10,
		Xmax: 30,
		Ymax: 30,
	}

	box2 := &Bbox{
		Xmin: 0,
		Ymin: 0,
		Xmax: 40,
		Ymax: 40,
	}

	require.Equal(t, box1.Intersects(box2), true)
}

func TestBboxIntersectsTrueAntimeridian(t *testing.T) {
	box1 := &Bbox{
		Xmin: 170,
		Ymin: -10,
		Xmax: -165,
		Ymax: 10,
	}

	box2 := &Bbox{
		Xmin: -180,
		Ymin: -5,
		Xmax: -170,
		Ymax: 15,
	}

	require.Equal(t, box1.Intersects(box2), true)
}

func TestBboxIntersectsFalseAntimeridian(t *testing.T) {
	box1 := &Bbox{
		Xmin: 170,
		Ymin: -10,
		Xmax: 180,
		Ymax: 10,
	}

	box2 := &Bbox{
		Xmin: -160,
		Ymin: -5,
		Xmax: -150,
		Ymax: 15,
	}

	require.Equal(t, box1.Intersects(box2), false)
}

func TestNewBboxFromString(t *testing.T) {
	bbox, err := NewBboxFromString("-160,-5,-150,15")
	assert.NoError(t, err)
	assert.Equal(t, -160.0, bbox.Xmin)
	assert.Equal(t, -5.0, bbox.Ymin)
	assert.Equal(t, -150.0, bbox.Xmax)
	assert.Equal(t, 15.0, bbox.Ymax)
}

func TestNewBboxFromStringErrNotEnoughValues(t *testing.T) {
	bbox, err := NewBboxFromString("-160,-5,-150")
	assert.ErrorContains(t, err, "please provide 4")
	assert.Nil(t, bbox)
}

func TestNewBboxFromStringErrWrongType(t *testing.T) {
	bbox, err := NewBboxFromString("foo,-5,-150,15")
	assert.ErrorContains(t, err, "float")
	assert.Nil(t, bbox)
}
