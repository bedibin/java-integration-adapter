DIR =	javaadapter \
	dbsyncplugin \
	queuedirplugin \
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


