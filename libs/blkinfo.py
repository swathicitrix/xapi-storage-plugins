import libiscsi
import urlparse
from xapi.storage.common import call

def get_device_path(dbg, uri):
    u = urlparse.urlparse(uri)

    if u.scheme == 'iscsi':
        dev_path = libiscsi.get_device_path(dbg, u)
    else:
        dev_path = "/%s%s" % (u.netloc, u.path)

    return dev_path


def get_format(dbg, dev_path):
    # FIXME:Check path exists

    cmd = ["/usr/sbin/blkid", "-s", "TYPE", dev_path]
    try:
        output = call(dbg, cmd)
        # output should look like
        # <dev_path>: TYPE="<type>"
        format_type = output.split(":")[1].split("=")[1].strip(' \t\n\r')
        format_type = format_type[1:-1]
    except:
        format_type = ""
    return format_type
