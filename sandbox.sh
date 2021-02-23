#!/bin/bash

N=${N:-4}

echo "Setup sandbox under prefix '${PREFIX}'"

if [ -z "$PREFIX" ]
then
	echo "No prefix directory was set."
	exit -1
fi

export BASE=$(realpath ${PREFIX})
if [ -z "$BASE" ]
then
	echo "Could not resolve absolute path of prefix directory"
	exit -1
fi


mkdir -p $BASE



if [[ ! -e $BASE/tbb/is_installed ]]; then
	echo "Installing TBB"
	if [ -d "$BASE/tbb/src" ]; then
		git  --work-tree=$BASE/tbb/src --git-dir=$BASE/tbb/src/.git pull origin master
	else
		mkdir -p $BASE/tbb/src || exit 1
		git clone https://github.com/wjakob/tbb.git $BASE/tbb/src || exit 1
	fi
	mkdir -p $BASE/tbb/build || exit 1
	cd $BASE/tbb/build || exit 1
	cmake -DCMAKE_INSTALL_PREFIX=$BASE/tbb/install -DCMAKE_BUILD_TYPE=Release $BASE/tbb/src || exit 1
	make -j$N || exit 1
	make install || exit 1
	mkdir $BASE/tbb/is_installed
fi

if [[ ! -e $BASE/dbengineparadigms/is_installed ]]; then
	echo "Installing DBEngineParadigms"
	if [ -d "$BASE/dbengineparadigms/src" ]; then
		git  --work-tree=$BASE/dbengineparadigms/src --git-dir=$BASE/dbengineparadigms/src/.git pull origin master
	else
		mkdir -p $BASE/dbengineparadigms/src || exit 1
		git clone https://github.com/t1mm3/db-engine-paradigms.git $BASE/dbengineparadigms/src || exit 1
	fi
	mkdir -p $BASE/dbengineparadigms/build || exit 1
	cd $BASE/dbengineparadigms/build || exit 1
	cmake -DCMAKE_INSTALL_PREFIX=$BASE/dbengineparadigms/install -DCMAKE_BUILD_TYPE=Release $BASE/dbengineparadigms/src -DTBB_ROOT_DIR=$BASE/tbb/install -DTBBROOT=$BASE/tbb/install -DTBB_INSTALL_DIR=$BASE/tbb/install -DTBB_LIBRARY=$BASE/tbb/install/lib || exit 1
	make -j$N || exit 1
	make install || exit 1
	mkdir $BASE/dbengineparadigms/is_installed
fi

if [[ ! -e $BASE/imv/is_installed ]]; then
	echo "Installing IMV"
	if [ -d "$BASE/imv/src" ]; then
		git  --work-tree=$BASE/imv/src --git-dir=$BASE/imv/src/.git pull origin master
	else
		mkdir -p $BASE/imv/src || exit 1
		git clone https://github.com/t1mm3/db-imv-clone.git $BASE/imv/src || exit 1
	fi
	mkdir -p $BASE/imv/build || exit 1
	cd $BASE/imv/build || exit 1
	cmake -DCMAKE_INSTALL_PREFIX=$BASE/imv/install -DCMAKE_BUILD_TYPE=Release $BASE/imv/src -DTBB_ROOT_DIR=$BASE/tbb/install -DTBBROOT=$BASE/tbb/install -DTBB_INSTALL_DIR=$BASE/tbb/install -DTBB_LIBRARY=$BASE/tbb/install/lib || exit 1
	make -j$N || exit 1
	make install || exit 1
	mkdir $BASE/imv/is_installed
fi

echo "Installing VOILA"

if [[ ! -e $BASE/voila/src ]]; then
	mkdir -p $BASE/voila/src || exit 1
	git clone git@github.com:t1mm3/vldb_voila.git $BASE/voila/src || exit 1
fi

mkdir -p $BASE/voila/debug || exit 1
cd $BASE/voila/debug || exit 1
echo "Make VOILA with using TBB from '$BASE/tbb/install'"
echo "cmake -DCMAKE_INSTALL_PREFIX=$BASE/voila/install -DCMAKE_BUILD_TYPE=Debug -DTBBROOT=$BASE/tbb/install -DTBB_INSTALL_DIR=$BASE/tbb/install $BASE/voila/src"
cmake -DCMAKE_INSTALL_PREFIX=$BASE/voila/install -DCMAKE_BUILD_TYPE=Debug   -DTBB_ROOT_DIR=$BASE/tbb/install -DTBBROOT=$BASE/tbb/install -DTBB_LIBRARY=$BASE/tbb/install/lib -DTBB_INSTALL_DIR=$BASE/tbb/install $BASE/voila/src  || exit 1
make -j$N || exit 1
make install || exit 1

mkdir -p $BASE/voila/release || exit 1
cd $BASE/voila/release || exit 1
echo "Make VOILA with using TBB from '$BASE/tbb/install'"
echo "cmake -DCMAKE_INSTALL_PREFIX=$BASE/voila/install -DCMAKE_BUILD_TYPE=Release -DTBBROOT=$BASE/tbb/install -DTBB_INSTALL_DIR=$BASE/tbb/install $BASE/voila/src"
cmake -DCMAKE_INSTALL_PREFIX=$BASE/voila/install -DCMAKE_BUILD_TYPE=Release -DTBB_ROOT_DIR=$BASE/tbb/install -DTBBROOT=$BASE/tbb/install -DTBB_LIBRARY=$BASE/tbb/install/lib -DTBB_INSTALL_DIR=$BASE/tbb/install $BASE/voila/src  || exit 1
make -j$N || exit 1
make install || exit 1


$BASE/voila/debug/voila --flavor vector -q q6 -s 1 -r 1
$BASE/voila/release/voila --flavor vector -q q6 -s 1 -r 1
