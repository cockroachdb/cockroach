#!/bin/bash

hg bookmark -f -r default master
hg push git+ssh://git@github.com/tebeka/go2xunit.git
