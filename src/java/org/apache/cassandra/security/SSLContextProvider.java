package org.apache.cassandra.security;

import java.io.IOException;
import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.SslContext;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * SSLContextProvider to allow SSL Context building to be pluggable.
 */
public interface SSLContextProvider
{
    /**
     * Create a Netty {@link SslContext}
     * @return returns Netty {@link SslContext}
     */
    public SslContext buildNettySSLContext(EncryptionOptions options, boolean buildTruststore,
                                           SSLFactory.SocketType socketType, boolean useOpenSsl) throws IOException;

    /**
     * Create a JSSE {@link SSLContext}.
     * @return returns JSEE {@link SSLContext}.
     */
    public SSLContext buildSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException;
}
