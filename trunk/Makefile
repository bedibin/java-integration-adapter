DIR =	javaadapter \
	dbsyncplugin \
	queuedirplugin \
	servicemanager \
	servlet \
	assetmanager \
	ucmdb

all:	$(DIR)

$(DIR)::
	cd $@; $(MAKE) $(MAKEFLAG)

clean::
	@$(MAKE) MAKEFLAG="clean"

lint::
	@$(MAKE) MAKEFLAG="lint"


