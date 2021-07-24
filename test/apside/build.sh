# build host os
go build -ldflags="-s -w" -o apside ./apside.go 


# build for android
export TOOLS=$HOME/Library/Android/sdk/ndk/21.3.6528147/toolchains/llvm/prebuilt/darwin-x86_64/bin
export GOOS=android
export GOARCH=arm
export CGO_ENABLED=1

export CC=${TOOLS}/armv7a-linux-androideabi21-clang
export CXX=${TOOLS}/armv7a-linux-androideabi21-clang++
go build -ldflags="-s -w" -o apside_armv7a ./apside.go 


export GOOS=android
export GOARCH=arm64
export CGO_ENABLED=1

export CC=${TOOLS}/aarch64-linux-android21-clang
export CXX=${TOOLS}/aarch64-linux-android21-clang++
go build -ldflags="-s -w" -o apside_aarch64 ./apside.go 

export GOOS=android
export GOARCH=386
export CGO_ENABLED=1

export CC=${TOOLS}/i686-linux-android21-clang
export CXX=${TOOLS}/i686-linux-android21-clang++
go build -ldflags="-s -w" -o apside_i686 ./apside.go 


export GOOS=android
export GOARCH=amd64
export CGO_ENABLED=1

export CC=${TOOLS}/x86_64-linux-android21-clang
export CXX=${TOOLS}/x86_64-linux-android21-clang++
go build -ldflags="-s -w" -o apside_x86_64 ./apside.go 