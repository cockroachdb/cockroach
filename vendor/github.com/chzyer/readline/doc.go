// Readline is a pure go implementation for GNU-Readline kind library.
//
// WHY: Readline will support most of features which GNU Readline is supported, and provide a pure go environment and a MIT license.
//
// example:
// 	rl, err := readline.New("> ")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer rl.Close()
//
// 	for {
// 		line, err := rl.Readline()
// 		if err != nil { // io.EOF
// 			break
// 		}
// 		println(line)
// 	}
//
package readline
