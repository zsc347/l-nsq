package httprouter

func CleanPath(p string) string {
	// Tern empty string into "/"
	if p == "" {
		return "/"
	}

	n := len(p)
	var buf []byte

	// path must start with '/'
	r := 1
	w := 1

	if p[0] != '/' {
		r = 0
		buf = make([]byte, n+1)
		buf[0] = '/'
	}

	trailing := n > 2 && p[n-1] == '/'

	for r < n {
		switch {
		case p[r] == '/':
			r++
		case p[r] == '.' && r+1 == n:
			trailing = true
			r++
		case p[r] == '.' && p[r+1] == '/':
			// .element
			r++
		case p[r] == '.' && p[r+1] == '.' && (r+2 == n || p[r+2] == '/'):
			// ..element : remove to last /
			r += 2
			if w > 1 {
				w--
				if buf == nil {
					for w > 1 && p[w] != '/' {
						w--
					}
				} else {
					for w > 1 && buf[w] != '/' {
						w--
					}
				}
			}
		default:
			// real path element
			// add slash if needed
			if w > 1 {
				bufApp(&buf, p, w, '/')
				w++
			}
			// copy element
			for r < n && p[r] != '/' {
				bufApp(&buf, p, w, p[r])
				w++
				r++
			}
		}
	}

	if trailing && w > 1 {
		bufApp(&buf, p, w, '/')
		w++
	}

	if buf == nil {
		return p[:w]
	}
	return string(buf[:w])
}

// internal helper to lazily create a buffer if necessary
func bufApp(buf *[]byte, s string, w int, c byte) {
	if *buf == nil {
		if s[w] == c {
			return
		}
		*buf = make([]byte, len(s))
		copy(*buf, s[:w])
	}
	(*buf)[w] = c
}
