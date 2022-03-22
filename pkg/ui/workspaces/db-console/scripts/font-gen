#!/bin/sh

# Remove previous build artifacts
rm -rf pkg/ui/fonts/Inconsolata
rm -rf pkg/ui/fonts/Lato
rm -f pkg/ui/dist/fonts.zip

# Download the necessary fonts from Google Fonts
curl -fsSL 'https://fonts.google.com/download?family=Lato|Inconsolata' > pkg/ui/dist/fonts.zip

# Extract the archive
unzip pkg/ui/dist/fonts.zip -d pkg/ui/fonts

# Convert each of the fonts we need
pkg/ui/scripts/font-convert pkg/ui/fonts/Inconsolata/Inconsolata-Regular.ttf
pkg/ui/scripts/font-convert pkg/ui/fonts/Lato/Lato-Regular.ttf
pkg/ui/scripts/font-convert pkg/ui/fonts/Lato/Lato-Bold.ttf

# Move the converted fonts + license files to their destinations
mv pkg/ui/fonts/Inconsolata/Inconsolata-Regular.woff* pkg/ui/fonts
mv pkg/ui/fonts/Inconsolata/OFL.txt licenses/OFL-inconsolata.txt

mv pkg/ui/fonts/Lato/Lato-*.woff* pkg/ui/fonts
mv pkg/ui/fonts/Lato/OFL.txt licenses/OFL-lato.txt

# Clean up after ourselves
rm -rf pkg/ui/fonts/Inconsolata
rm -rf pkg/ui/fonts/Lato
rm -f pkg/ui/dist/fonts.zip
