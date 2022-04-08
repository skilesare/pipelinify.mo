let upstream = https://github.com/dfinity/vessel-package-set/releases/download/mo-0.6.21-20220215/package-set.dhall sha256:b46f30e811fe5085741be01e126629c2a55d4c3d6ebf49408fb3b4a98e37589b
let Package =
    { name : Text, version : Text, repo : Text, dependencies : List Text }

let additions =
    [
   { name = "candy"
   , repo = "https://github.com/aramakme/candy_library.git"
   , version = "v0.1.5"
   , dependencies = ["base"]
   },
   { name = "principal"
   , repo = "https://github.com/aviate-labs/principal.mo.git"
   , version = "v0.1.1"
   , dependencies = ["base"]
   }] : List Package

let
  {- This is where you can override existing packages in the package-set

     For example, if you wanted to use version `v2.0.0` of the foo library:
     let overrides = [
         { name = "foo"
         , version = "v2.0.0"
         , repo = "https://github.com/bar/foo"
         , dependencies = [] : List Text
         }
     ]
  -}
  overrides =
    [] : List Package


in  upstream # additions # overrides
