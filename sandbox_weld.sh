#!/bin/env bash


# Install rust using:
# curl https://sh.rustup.rs -sSf | sh

export BASE=/export/scratch1/home/tgubner/monet2
export N=32

# LLVM60
if [[ ! -e $BASE/llvm60/is_installed ]]; then
	echo "Installing LLVM6"
	mkdir -p $BASE/llvm60/src
	git clone -b release/6.x https://github.com/llvm/llvm-project.git $BASE/llvm60/src  || exit 1
	mkdir -p $BASE/llvm60/install
	mkdir -p $BASE/llvm60/build

	cd $BASE/llvm60/build

	cmake -DLLVM_ENABLE_PROJECTS="clang;libcxx;libcxxabi" -DCMAKE_INSTALL_PREFIX=$BASE/llvm60/install -DCMAKE_BUILD_TYPE=Release $BASE/llvm60/src/llvm || exit 1
	make -j$N || exit 1
	make install || exit 1

	cd $BASE/llvm60/install/bin
	ln -s clang++ clang++-6.0
	mkdir $BASE/llvm60/is_installed
fi

export PATH=$BASE/llvm60/install/bin:$PATH
echo "" > $BASE/env.sh
echo "export LLVM60_BIN=$BASE/llvm60/install/bin" >> $BASE/env.sh


# Get Weld
if [[ ! -e $BASE/weld/is_installed ]]; then
	git clone https://github.com/t1mm3/weld.git $BASE/weld || exit 1
	cd $BASE/weld
	cargo build --release || exit 1
	cargo test || exit 1

	mkdir -p $BASE/weld/install
	mkdir -p $BASE/weld/install/include
	mkdir -p $BASE/weld/install/lib

	cp -rf $BASE/weld/weld-capi/weld.h $BASE/weld/install/include
	cp -rf $BASE/weld/target/release/libweld.so $BASE/weld/install/lib

	mkdir $BASE/weld/is_installed
fi

export WELD_HOME=$BASE/weld
echo "export WELD_HOME=$BASE/weld" >> $BASE/env.sh
echo "export WELD_LIB=$BASE/weld/install/lib" >> $BASE/env.sh

# Get MonetDB/Weld
if [[ ! -e $BASE/monet-weld/is_installed ]]; then
	mkdir -p $BASE/monet-weld/src
	hg clone -r rel-weld https://dev.monetdb.org/hg/MonetDB/ $BASE/monet-weld/src

	cd $BASE/monet-weld/src

	# optimized build according to Mihai
	./bootstrap || exit 1
	./configure --prefix=$BASE/monet-weld/install --disable-strict --disable-assert --disable-debug --enable-optimize --with-weld=$BASE/weld/install --disable-int128  || exit 1

	make -j$N || exit 1
	make install || exit 1

	mkdir $BASE/monet-weld/is_installed
fi

export PATH=$BASE/monet-weld/install/bin:$PATH
export LD_LIBRARY_PATH=$BASE/weld/install/lib:$BASE/monet-weld/install/lib:$LD_LIBRARY_PATH
echo "export MONET_WELD_BIN=$BASE/monet-weld/install/bin" >> $BASE/env.sh
echo "export MONET_WELD_LIB=$BASE/monet-weld/install/lib" >> $BASE/env.sh


# Get tpchtools
if [[ ! -e $BASE/monet-weld-tpch/is_installed ]]; then
	git clone --recurse-submodules https://github.com/t1mm3/tpch-tools.git $BASE/monet-weld-tpch

	cd $BASE/monet-weld-tpch/scripts/

	./setup-tpch-db -s 1 -d tpch_sf1 -f $BASE/monetfarm/ || exit 1

	mkdir $BASE/monet-weld-tpch/is_installed
fi



# Update environment's paths
echo 'export PATH=$LLVM60_BIN:$MONET_WELD_BIN:$PATH' >> $BASE/env.sh
echo 'export LD_LIBRARY_PATH=$WELD_LIB:$MONET_WELD_LIB:$LD_LIBRARY_PATH' >> $BASE/env.sh


echo "Please update your environment variables"
echo "source $BASE/env.sh"