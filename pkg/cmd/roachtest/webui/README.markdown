# WebUI RoachTests

## Setup

### Install:

- [Selenium server](https://www.seleniumhq.org/download/)
  - (already in the `vendored` package)
- [ChromeDriver](https://sites.google.com/a/chromium.org/chromedriver/downloads)
  - (perhaps try `brew cask install chromedriver`)
- [GeckoDriver](https://github.com/mozilla/geckodriver)
  - ( perhaps try `brew install geckodriver`)

### Run:

In one terminal, do:

```
> java -jar vendor/seleniumhq.org/selenium-server-stanlone-3.12.0.jar
```

And in another, try:

```
./bin/roachtest run webui --local --cockroach ./cockroach --workload ./bin/workload --artifacts artifacts
```
