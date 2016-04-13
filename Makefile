LIBS_FILES=__init__.py device.py iscsi.py losetup.py tapdisk.py dmsetup.py nbdclient.py nbdtool.py image.py libvhd.py vhd_coalesce.py poolhelper.py libiscsi.py scsiutil.py
PLUGINS=suspend-resume-datapath gfs2setup

.PHONY: clean
clean:

DESTDIR?=/
SCRIPTDIR?=/usr/libexec/xapi-storage-script
PYTHONDIR?=/usr/lib/python2.7/site-packages/xapi/storage/libs
PLUGINSDIR?=/etc/xapi.d/plugins

install:
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/datapath/loop+blkback
	(cd datapath/loop+blkback; install -m 0755 plugin.py datapath.py $(DESTDIR)$(SCRIPTDIR)/datapath/loop+blkback)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/loop+blkback; for link in Datapath.attach Datapath.activate Datapath.deactivate Datapath.detach; do ln -s datapath.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/loop+blkback; for link in Plugin.Query; do ln -s plugin.py $$link; done)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/datapath/tapdisk
	(cd datapath/tapdisk; install -m 0755 plugin.py datapath.py $(DESTDIR)$(SCRIPTDIR)/datapath/tapdisk)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/tapdisk; for link in Datapath.open Datapath.attach Datapath.activate Datapath.deactivate Datapath.detach Datapath.close; do ln -s datapath.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/tapdisk; for link in Plugin.Query; do ln -s plugin.py $$link; done)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/datapath/vhd+tapdisk
	(cd datapath/vhd+tapdisk; install -m 0755 plugin.py datapath.py $(DESTDIR)$(SCRIPTDIR)/datapath/vhd+tapdisk)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/vhd+tapdisk; for link in Datapath.open Datapath.attach Datapath.activate Datapath.deactivate Datapath.detach Datapath.close; do ln -s datapath.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/vhd+tapdisk; for link in Plugin.Query; do ln -s plugin.py $$link; done)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/datapath/raw+block
	(cd datapath/raw+block; install -m 0755 plugin.py datapath.py $(DESTDIR)$(SCRIPTDIR)/datapath/raw+block)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/raw+block; for link in Datapath.attach Datapath.activate Datapath.deactivate Datapath.detach; do ln -s datapath.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath/raw+block; for link in Plugin.Query; do ln -s plugin.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/datapath ; ln -snf tapdisk raw+file ; ln -snf tapdisk vhd+file)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.gfs2
	(cd volume/org.xen.xapi.storage.gfs2; install -m 0755 gfs2.py plugin.py sr.py volume.py $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.gfs2)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.gfs2; for link in Plugin.diagnostics Plugin.Query; do ln -s plugin.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.gfs2; for link in Volume.destroy Volume.set_description Volume.stat Volume.clone Volume.resize Volume.set_name Volume.unset Volume.create Volume.set Volume.snapshot; do ln -s volume.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.gfs2; for link in SR.destroy SR.stat SR.attach SR.detach SR.create SR.ls ; do ln -s sr.py $$link; done)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.lvm2
	(cd volume/org.xen.xapi.storage.lvm2; install -m 0755 plugin.py sr.py volume.py $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.lvm2)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.lvm2; for link in Plugin.diagnostics Plugin.Query; do ln -s plugin.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.lvm2; for link in Volume.destroy Volume.set_description Volume.stat Volume.clone Volume.resize Volume.set_name Volume.unset Volume.create Volume.set Volume.snapshot; do ln -s volume.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.lvm2; for link in SR.destroy SR.stat SR.attach SR.detach SR.create SR.ls ; do ln -s sr.py $$link; done)
	mkdir -p $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.ffs
	(cd volume/org.xen.xapi.storage.ffs; install -m 0755 plugin.py sr.py volume.py $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.ffs)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.ffs; for link in Plugin.diagnostics Plugin.Query; do ln -s plugin.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.ffs; for link in Volume.destroy Volume.set_description Volume.stat Volume.clone Volume.resize Volume.set_name Volume.unset Volume.create Volume.set Volume.snapshot; do ln -s volume.py $$link; done)
	(cd $(DESTDIR)$(SCRIPTDIR)/volume/org.xen.xapi.storage.ffs; for link in SR.destroy SR.stat SR.attach SR.detach SR.create SR.ls ; do ln -s sr.py $$link; done)	
	mkdir -p $(DESTDIR)$(PYTHONDIR)
	(cd libs; install -m 0755 $(LIBS_FILES) $(DESTDIR)$(PYTHONDIR)/)
	mkdir -p $(DESTDIR)$(PLUGINSDIR)
	(cd overlay/etc/xapi.d/plugins; install -m 0755 $(PLUGINS) $(DESTDIR)$(PLUGINSDIR)/)
