DIR =	javaadapter \
	dbsyncplugin \
	queuedirplugin \
	servlet \
	assetmanager \
	ucmdb \
	servicemanager \
	excelreader \
	jmsplugins \
	cryptoplugins

all:	$(DIR)

$(DIR)::
	cd $@; $(MAKE) $(MAKEFLAG)

clean::
	@$(MAKE) MAKEFLAG="clean"

lint::
	@$(MAKE) MAKEFLAG="lint"


