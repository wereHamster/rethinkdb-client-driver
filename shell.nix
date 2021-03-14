let
  pkgs = import <nixpkgs> {};

in pkgs.mkShell {
  buildInputs = [
    pkgs.stack
    pkgs.zlib
    pkgs.libiconv
  ];
}
