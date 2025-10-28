package utils

import (
	"fmt"
	"math"
	"regexp"
)

// drawLine connects two grid points roughly with char c.
func drawLine(grid [][]rune, x1, y1, x2, y2 int, c rune) {
	dx := abs(x2 - x1)
	dy := abs(y2 - y1)
	sx := -1
	if x1 < x2 {
		sx = 1
	}
	sy := -1
	if y1 < y2 {
		sy = 1
	}
	err := dx - dy
	for {
		if y1 >= 0 && y1 < len(grid) && x1 >= 0 && x1 < len(grid[y1]) {
			grid[y1][x1] = c
		}
		if x1 == x2 && y1 == y2 {
			break
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x1 += sx
		}
		if e2 < dx {
			err += dx
			y1 += sy
		}
	}
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// shortTwoDigits extracts the last two digits for labels.
// Example: "fa25-cs425-3306@cs.illinois.edu" -> "06".
func shortTwoDigits(s string) string {
	// Prefer 4 digits before '@' or end, e.g., "-3306@" or "-3306"
	if m := regexp.MustCompile(`-(\d{4})(?:@|$)`).FindStringSubmatch(s); len(m) == 2 {
		return m[1][2:] // last two of the 4 digits
	}
	// Otherwise: last two consecutive digits anywhere
	all := regexp.MustCompile(`(\d{2,})`).FindAllStringSubmatch(s, -1)
	if len(all) > 0 {
		last := all[len(all)-1][1]
		if len(last) >= 2 {
			return last[len(last)-2:]
		}
	}
	// Fallback: "??"
	return "??"
}

// PrintAsciiRingWithLegend prints the ring; labels are the last two digits (e.g., 06).
func PrintAsciiRingWithLegend(nodes []string, ringNodesIDs []string, ringNodeTokens []uint64) {
	n := len(nodes)
	if n == 0 {
		fmt.Println("(no nodes)")
		return
	}
	if len(ringNodesIDs) < n {
		ringNodesIDs = append(ringNodesIDs, make([]string, n-len(ringNodesIDs))...)
	}

	// Slightly bigger canvas to avoid clipping labels at the edge
	size := 21
	radius := float64(size-3) / 2 // leave a margin for 2-char labels
	center := size / 2

	// empty grid
	grid := make([][]rune, size)
	for i := range grid {
		grid[i] = make([]rune, size)
		for j := range grid[i] {
			grid[i][j] = ' '
		}
	}

	type pt struct{ x, y int }
	positions := make([]pt, n)
	for i := 0; i < n; i++ {
		theta := 2 * math.Pi * float64(i) / float64(n)
		x := int(math.Round(radius*math.Cos(theta))) + center
		y := int(math.Round(radius*math.Sin(theta))) + center
		positions[i] = pt{x, y}
	}

	// draw circle segments
	for i := 0; i < n; i++ {
		a := positions[i]
		b := positions[(i+1)%n]
		drawLine(grid, a.x, a.y, b.x, b.y, '-')
	}

	// overlay node labels (two chars), centered horizontally on the point
	for i, p := range positions {
		lbl := []rune(shortTwoDigits(nodes[i]))
		var x0 = p.x - (len(lbl) / 2)
		for k, r := range lbl {
			x := x0 + k
			if p.y >= 0 && p.y < size && x >= 0 && x < size {
				grid[p.y][x] = r
			}
		}
	}

	// print grid
	for _, row := range grid {
		fmt.Println(string(row))
	}
	fmt.Println()

	// legend
	fmt.Println("Legend:")
	for i, node := range nodes {
		fmt.Printf("  %s → %s → %s → %d \n", shortTwoDigits(node), node, ringNodesIDs[i], ringNodeTokens[i])
	}
	fmt.Println()
}
