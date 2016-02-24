

#Gocolorize
Gocolorize is a package that allows Go programs to provide ANSI coloring in a stateful manner. Gocolorize is ideal for logging or cli applications.

![colored tests passing](https://raw.github.com/agtorre/gocolorize/master/screenshot/tests.png)

[![wercker status](https://app.wercker.com/status/ee73c1abee9900c475def6ff9a142237/s "wercker status")](https://app.wercker.com/project/bykey/ee73c1abee9900c475def6ff9a142237)


##Features
- Stateful ANSI coloring 
- Supports Foreground and background colors
- Supports a nuber of text properties such as bold or underline
- Color multiple arguments
- Color multiple interfaces, including complex types
- Tests with 100% coverage
- Working examples
- Disable ability for portability


##Install Gocolorize
To install:

    $ go get github.com/agtorre/gocolorize

##Usage
Ways to initialize a Colorize object:
```go
    //It can be done like this
    var c gocolorize.Colorize
    c.SetFg(gocolorize.Red)
    c.SetBg(gocolorize.Black)
    
    //Or this
    c := gocolorize.Colorize{Fg: gocolorize.Red, Bg: gocolorize.Black}
    
    //Or this
    c := gocolorize.NewColor("red:black")
    
```

Once you have an object:
```go
    //Call Paint to take inputs and return a colored string
    c.Paint("This", "accepts", "multiple", "arguments", "and", "types:", 1, 1.25, "etc")

    //If you want a short-hand closure
    p = c.Paint
    p("Neat")

    //To print it:
    Fmt.Println(p("test"))
    
    //It can also be appended to other strings, used in logging to stdout, etc.
    a := "test " + p("case")

    //The closure allows you to reuse the original object, for example
    p = c.Paint
    c.SetFg(gocolorize.Green)
    p2 = c.Paint
    Fmt.Println(p("different" + " " + p2("colors")))
```

Object Properties:
```go
    //These will only apply if there is a Fg and Bg respectively
    c.ToggleFgIntensity()
    c.ToggleBgIntensity()
    
    //Set additional attributes
    c.ToggleBold()
    c.ToggleBlink()
    c.ToggleUnderLine()
    c.ToggleInverse()

    //To disable or renable everything color (for example on Windows)
    //the other functions will still work, they'll just return plain
    //text for portability
    gocolorize.SetPlain(true)
```

##NewColor String Format
```go    
"foregroundColor+attributes:backgroundColor+attributes"
```

Colors:
* black
* red
* green
* yellow
* blue
* magenta
* cyan
* white

Attributes:
* b = bold foreground
* B = blink foreground
* u = underline foreground
* h = high intensity (bright) foreground, background
* i = inverse


##Examples
See examples directory for examples:

    $ cd examples/
    $ go run song.go
    $ go run logging.go
    

##Tests
Tests are another good place to see examples. In order to run tests:

    $ go test -cover

##Portability
ANSI coloring will not work in default Windows environments and may not work in other environments correctly. In order to allow compatibility with these environments, you can call:

```go
gocolorize.SetPlain(true)
```

Once toggled, the library will still function, but it will not color the output.
    
## References

Wikipedia ANSI escape codes [Colors](http://en.wikipedia.org/wiki/ANSI_escape_code#Colors)

[A stylesheet author's guide to terminal colors](http://wynnnetherland.com/journal/a-stylesheet-author-s-guide-to-terminal-colors)

## Special Thanks
https://github.com/mgutz/ansi was an inspiration for the latest version. I did a lot of rewriting, removed the 'paints' module, and did a lot of general cleanup. I learned a lot reading this and using this code.
