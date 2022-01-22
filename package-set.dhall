let upstream = https://github.com/dfinity/vessel-package-set/releases/download/mo-0.6.4-20210624/package-set.dhall sha256:3f4cffd315d8ee5d2b4b5b00dc03b2e02732345b565340b7cb9cc0001444f525
let Package =
    { name : Text, version : Text, repo : Text, dependencies : List Text }

let additions =
    [
   { name = "candy"
   , repo = "https://github.com/aramakme/candy_library.git"
   , version = "v0.1.1"
   , dependencies = ["base"]
   },
   { name = "principal"
   , repo = "https://github.com/aviate-labs/principal.mo.git"
   , version = "v0.1.1"
   , dependencies = ["base"]
   }] : List Package

let overrides =
    [{
    name = "base",
    repo = "https://github.com/dfinity/motoko-base",
    version = "dfx-0.7.2",
    dependencies = [] : List Text
  }] : List Package

in  upstream # additions # overrides
