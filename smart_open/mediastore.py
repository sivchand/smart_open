
import logging
import urllib.parse
import io

try:
    import boto3
    import botocore.client
    import botocore.exceptions
    import urllib3.exceptions
except ImportError:
    MISSING_DEPS = True

from smart_open import constants
import smart_open

logger = logging.getLogger(__name__)

SCHEMES = ('mediastore')
DEFAULT_PORT = 443

DEFAULT_BUFFER_SIZE = 128 * 1024

URI_EXAMPLES = (
    'mediastore://XXXXXX.data.mediastore.REGION.amazonaws.com/my_key',
    'mediastore://access_key:access_secret@XXXXXX.data.mediastore.REGION.amazonaws.com/my_key',
    'mediastore://XXXXXX.data.mediastore.REGION.amazonaws.com/my_key?CacheControl=no-cache&ContentType=audio/mpeg&UploadAvailability=STREAMING'
)


def parse_uri(uri_as_str):
    """Parse the specified URI into a dict.

    At a bare minimum, the dict must have `schema` member.
    """
    split_uri = urllib.parse.urlsplit(uri_as_str, allow_fragments=False)
    assert split_uri.scheme in SCHEMES

    port = DEFAULT_PORT
    access_id, access_secret = None, None

    uri = split_uri.netloc + split_uri.path

    if '@' in uri and ':' in uri.split('@')[0]:
        auth, uri = uri.split('@', 1)
        access_id, access_secret = auth.split(':')

    head, key_id = uri.split('/', 1)
    if ':' in head:
        host, port = head.split(':', 1)
        port = int(port)
    else:
        host = head

    qs = dict(urllib.parse.parse_qsl(split_uri.query))

    retval = dict(
        scheme=split_uri.scheme,
        host=host,
        port=port,
        key_id=key_id,
        access_id=access_id,
        access_secret=access_secret,
        object_kwargs=dict(
            CacheControl=qs.get('CacheControl', 'no-cache'),
            ContentType=qs.get(
                'ContentType', 'application/octet-stream'),
            UploadAvailability=qs.get(
                'UploadAvailability', 'STREAMING'))
    )

    return retval


def _consolidate_params(uri, transport_params):
    """Consolidates the parsed Uri with the additional parameters.

    This is necessary because the user can pass some of the parameters can in
    two different ways:

    1) Via the URI itself
    2) Via the transport parameters

    These are not mutually exclusive, but we have to pick one over the other
    in a sensible way in order to proceed.

    """
    transport_params = dict(transport_params)

    def inject(**kwargs):
        try:
            client_kwargs = transport_params['client_kwargs']
        except KeyError:
            client_kwargs = transport_params['client_kwargs'] = {}

        try:
            init_kwargs = client_kwargs['MediaStoreData.Client']
        except KeyError:
            init_kwargs = client_kwargs['MediaStoreData.Client'] = {}

        init_kwargs.update(**kwargs)

    client = transport_params.get('client')
    if client is not None and (uri['access_id'] or uri['access_secret']):
        logger.warning(
            'ignoring credentials parsed from URL because they conflict with '
            'transport_params["client"]. Set transport_params["client"] to None '
            'to suppress this warning.'
        )
        uri.update(access_id=None, access_secret=None)
    elif (uri['access_id'] and uri['access_secret']):
        inject(
            aws_access_key_id=uri['access_id'],
            aws_secret_access_key=uri['access_secret'],
        )
        uri.update(access_id=None, access_secret=None)

    inject(endpoint_url='https://%(host)s:%(port)d' % uri)
    uri.update(host=None)

    return uri, transport_params


def open_uri(uri_as_str, mode, transport_params):
    """Return a file-like object pointing to the URI.

    Parameters:

    uri_as_str: str
        The URI to open
    mode: str
        Either "rb" or "wb".  You don't need to implement text modes,
        `smart_open` does that for you, outside of the transport layer.
    transport_params: dict
        Any additional parameters to pass to the `open` function (see below).

    """
    parsed_uri = parse_uri(uri_as_str)
    parsed_uri, transport_params = _consolidate_params(
        parsed_uri, transport_params)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    print(parsed_uri)
    print(transport_params)
    return open(parsed_uri['key_id'], mode,
                object_kwargs=parsed_uri['object_kwargs'], **kwargs)


def open(
    key_id,
    mode,
    object_kwargs=None,
    client=None,
    client_kwargs=None,
    writebuffer=None
):
    """This function does the hard work.

    The keyword parameters are the transport_params from the `open_uri`
    function.

    """
    if mode not in constants.BINARY_MODES:
        raise NotImplementedError(
            'bad mode: %r expected one of %r' % (mode, constants.BINARY_MODES))

    if mode == constants.WRITE_BINARY:
        fileobj = Writer(key_id,
                         client=client,
                         client_kwargs=client_kwargs,
                         object_kwargs=object_kwargs,
                         writebuffer=writebuffer
                         )
    else:
        assert False, 'unexpected mode: %r' % mode

    fileobj.name = key_id
    return fileobj


def _initialize_boto3(rw, client, client_kwargs, key):
    """Created the required objects for accessing S3.  Ideally, they have
    been already created for us and we can just reuse them."""
    if client_kwargs is None:
        client_kwargs = {}

    if client is None:
        init_kwargs = client_kwargs.get('MediaStoreData.Client', {})
        client = boto3.client('mediastore-data', **init_kwargs)
    assert client

    rw._client = client
    rw._key = key


class Writer(io.BufferedIOBase):

    def __init__(
            self,
            key,
            client=None,
            client_kwargs=None,
            object_kwargs=None,
            writebuffer=None
    ):

        _initialize_boto3(self, client, client_kwargs, key)
        self._object_kwargs = object_kwargs
        if writebuffer is None:
            self._buf = io.BytesIO()
        else:
            self._buf = writebuffer

        self._total_bytes = 0

        self.raw = None

    def flush(self):
        pass

    def close(self):
        if self._buf is None:
            return

        self._buf.seek(0)

        try:
            self._client.put_object(
                Body=self._buf,
                Path=self._key,
                **self._object_kwargs
            )
        except botocore.client.ClientError as e:
            raise ValueError(
                'access is forbidden or not the mediastore container does not exists') from e

    @property
    def closed(self):
        return self._buf is None

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only tell support, and no seek or truncate support."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_bytes

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def seek(self, offset, whence=constants.WHENCE_START):
        """Unsupported."""
        raise io.UnsupportedOperation

    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        length = self._buf.write(b)
        self._total_bytes += length
        return length

    def terminate(self):
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()


print(open_uri("mediastore://my_host/key", "wb", {}))
