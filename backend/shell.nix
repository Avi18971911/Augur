{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.go
    pkgs.git
    pkgs.nodejs
    pkgs.nodePackages.npm
  ];

  GO_VERSION = "1.22.2";

  shellHook = ''
    export GOROOT=/nix/store/rfcwglhhspqx5v5h0sl4b3py14i6vpxa-go-1.22.7/share/go
    export GOPATH=$HOME/go
    export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
    echo "Go environment loaded with GOROOT=$GOROOT and GOPATH=$GOPATH"
  '';
}
