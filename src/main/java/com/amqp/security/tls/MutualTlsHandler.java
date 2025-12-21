package com.amqp.security.tls;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.security.cert.X509Certificate;

/**
 * Handler for extracting client certificate information after TLS handshake.
 *
 * This handler should be placed after the SslHandler in the pipeline.
 * It extracts the client certificate (if present) and makes it available
 * for authentication via SASL EXTERNAL mechanism.
 */
@ChannelHandler.Sharable
public class MutualTlsHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MutualTlsHandler.class);

    public static final AttributeKey<X509Certificate> CLIENT_CERT_KEY =
            AttributeKey.valueOf("client.certificate");
    public static final AttributeKey<String> CLIENT_CERT_CN_KEY =
            AttributeKey.valueOf("client.certificate.cn");
    public static final AttributeKey<String> CLIENT_CERT_USER_KEY =
            AttributeKey.valueOf("client.certificate.user");
    public static final AttributeKey<Boolean> TLS_AUTHENTICATED_KEY =
            AttributeKey.valueOf("tls.authenticated");

    private final boolean requireClientCert;

    public MutualTlsHandler() {
        this(false);
    }

    public MutualTlsHandler(boolean requireClientCert) {
        this.requireClientCert = requireClientCert;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent) {
            SslHandshakeCompletionEvent sslEvent = (SslHandshakeCompletionEvent) evt;

            if (sslEvent.isSuccess()) {
                handleSuccessfulHandshake(ctx);
            } else {
                handleFailedHandshake(ctx, sslEvent.cause());
            }
        }

        super.userEventTriggered(ctx, evt);
    }

    private void handleSuccessfulHandshake(ChannelHandlerContext ctx) {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            log.warn("SslHandler not found in pipeline");
            return;
        }

        SSLSession session = sslHandler.engine().getSession();

        // Extract client certificate
        X509Certificate clientCert = CertificateManager.getClientCertificate(session);

        if (clientCert != null) {
            // Store certificate info in channel attributes
            ctx.channel().attr(CLIENT_CERT_KEY).set(clientCert);

            String cn = CertificateManager.getCommonName(clientCert);
            if (cn != null) {
                ctx.channel().attr(CLIENT_CERT_CN_KEY).set(cn);
            }

            String username = CertificateManager.extractUsername(clientCert);
            if (username != null) {
                ctx.channel().attr(CLIENT_CERT_USER_KEY).set(username);
            }

            ctx.channel().attr(TLS_AUTHENTICATED_KEY).set(true);

            log.info("Client certificate authenticated: CN={}, user={}",
                    cn, username);

            // Log certificate details
            CertificateManager.CertificateInfo info =
                    CertificateManager.getCertificateInfo(clientCert);
            if (info != null) {
                log.debug("Client certificate details: {}", info);
            }

        } else if (requireClientCert) {
            log.warn("Client certificate required but not provided, closing connection");
            ctx.close();
        } else {
            log.debug("No client certificate provided (mTLS not required)");
            ctx.channel().attr(TLS_AUTHENTICATED_KEY).set(false);
        }
    }

    private void handleFailedHandshake(ChannelHandlerContext ctx, Throwable cause) {
        log.error("TLS handshake failed: {}", cause.getMessage());
        ctx.close();
    }

    /**
     * Get the client certificate from a channel.
     */
    public static X509Certificate getClientCertificate(ChannelHandlerContext ctx) {
        return ctx.channel().attr(CLIENT_CERT_KEY).get();
    }

    /**
     * Get the client certificate's Common Name.
     */
    public static String getClientCertificateCN(ChannelHandlerContext ctx) {
        return ctx.channel().attr(CLIENT_CERT_CN_KEY).get();
    }

    /**
     * Get the username extracted from the client certificate.
     */
    public static String getClientCertificateUser(ChannelHandlerContext ctx) {
        return ctx.channel().attr(CLIENT_CERT_USER_KEY).get();
    }

    /**
     * Check if the connection was authenticated via TLS client certificate.
     */
    public static boolean isTlsAuthenticated(ChannelHandlerContext ctx) {
        Boolean authenticated = ctx.channel().attr(TLS_AUTHENTICATED_KEY).get();
        return authenticated != null && authenticated;
    }
}
