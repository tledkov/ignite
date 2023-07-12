/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ssl;

import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;

/** */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default key / trust store type. */
    public static final String DFLT_STORE_TYPE = System.getProperty("javax.net.ssl.keyStoreType", "JKS");

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Default key manager / trust manager algorithm. Specifying different trust manager algorithm is not supported. */
    public static final String DFLT_KEY_ALGORITHM = System.getProperty("ssl.KeyManagerFactory.algorithm", "SunX509");

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Key manager algorithm. */
    protected String keyAlgorithm = DFLT_KEY_ALGORITHM;

    /** Trust managers. */
    protected TrustManager[] trustMgrs;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Cached instance of an {@link SSLContext}. */
    protected final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();

    protected final KeyManagersFactory keyManagersFactory;
    protected final TrustManagersFactory trustManagersFactory;

    /** */
    public AbstractSslContextFactory(KeyManagersFactory keyManagersFactory, TrustManagersFactory trustManagersFactory) {
        this.keyManagersFactory = keyManagersFactory;
        this.trustManagersFactory = trustManagersFactory;
    }

    /**
     * Gets protocol for secure transport.
     *
     * @return SSL protocol name.
     */
    public String getProtocol() {
        return proto;
    }

    /**
     * Sets protocol for secure transport. If not specified, {@link #DFLT_SSL_PROTOCOL} will be used.
     *
     * @param proto SSL protocol name.
     */
    public void setProtocol(String proto) {
        A.notNull(proto, "proto");

        this.proto = proto;
    }

    /**
     * Gets algorithm that will be used to create a key manager. If not specified, {@link #DFLT_KEY_ALGORITHM}
     * will be used.
     *
     * @return Key manager algorithm.
     */
    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Sets key manager algorithm that will be used to create a key manager.
     *
     * @param keyAlgorithm Key algorithm name.
     */
    public void setKeyAlgorithm(String keyAlgorithm) {
        A.notNull(keyAlgorithm, "keyAlgorithm");

        this.keyAlgorithm = keyAlgorithm;
    }


    /**
     * Gets pre-configured trust managers.
     *
     * @return Trust managers.
     */
    public TrustManager[] getTrustManagers() {
        return trustMgrs;
    }

    /**
     * Sets pre-configured trust managers. This is an optional parameter,
     * however one of the {@link #setTrustStoreFilePath(String)}, {@code #setTrustManagers(TrustManager[])}
     *
     * @param trustMgrs Pre-configured trust managers.
     */
    public void setTrustManagers(TrustManager... trustMgrs) {
        this.trustMgrs = trustMgrs;
    }

    /**
     * Sets enabled cipher suites.
     *
     * @param cipherSuites enabled cipher suites.
     */
    public void setCipherSuites(String... cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    /**
     * Gets enabled cipher suites.
     *
     * @return enabled cipher suites
     */
    public String[] getCipherSuites() {
        return cipherSuites;
    }

    /**
     * Gets enabled protocols.
     *
     * @return Enabled protocols.
     */
    public String[] getProtocols() {
        return protocols;
    }

    /**
     * Sets enabled protocols.
     *
     * @param protocols Enabled protocols.
     */
    public void setProtocols(String... protocols) {
        this.protocols = protocols;
    }

    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    private SSLContext createSslContext() throws SSLException {
        checkParameters();

        KeyManager[] keyMgrs = keyManagersFactory.create(keyAlgorithm) ;

        TrustManager[] trustMgrs = this.trustMgrs;

        if (trustMgrs == null)
            trustMgrs = trustManagersFactory.create(keyAlgorithm);

        try {
            SSLContext ctx = SSLContext.getInstance(proto);

            if (cipherSuites != null || protocols != null) {
                SSLParameters sslParameters = new SSLParameters();

                if (cipherSuites != null)
                    sslParameters.setCipherSuites(cipherSuites);

                if (protocols != null)
                    sslParameters.setProtocols(protocols);

                ctx = new SSLContextWrapper(ctx, sslParameters);
            }

            ctx.init(keyMgrs, trustMgrs, null);

            return ctx;
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported SSL protocol: " + proto, e);
        }
        catch (KeyManagementException e) {
            throw new SSLException("Failed to initialized SSL context.", e);
        }
    }

    /** @throws SSLException If check failed. */
    protected abstract void checkParameters() throws SSLException;

    /**
     * @param param Value.
     * @param name Name.
     * @throws SSLException If {@code null}.
     */
    protected void checkNullParameter(Object param, String name) throws SSLException {
        if (param == null)
            throw new SSLException("Failed to initialize SSL context (parameter cannot be null): " + name);
    }

    /**
     * By default, this method simply opens a raw file input stream. Subclasses may override this method
     * if some specific location should be handled (this may be a case for Android users).
     *
     * @param filePath Path to the file.
     * @return Opened input stream.
     * @throws IOException If stream could not be opened.
     */
    @Deprecated
    protected InputStream openFileInputStream(String filePath) throws IOException {
        return new FileInputStream(filePath);
    }

    /**
     * Disabled trust manager, will skip all certificate checks.
     */
    protected static class DisabledX509TrustManager implements X509TrustManager {
        /** Empty certificate array. */
        private static final X509Certificate[] CERTS = new X509Certificate[0];

        /** {@inheritDoc} */
        @Override public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            // No-op, all clients are trusted.
        }

        /** {@inheritDoc} */
        @Override public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            // No-op, all servers are trusted.
        }

        /** {@inheritDoc} */
        @Override public X509Certificate[] getAcceptedIssuers() {
            return CERTS;
        }
    }

    /** {@inheritDoc} */
    @Override public SSLContext create() {
        SSLContext ctx = sslCtx.get();

        if (ctx == null) {
            try {
                ctx = createSslContext();

                if (!sslCtx.compareAndSet(null, ctx))
                    ctx = sslCtx.get();
            }
            catch (SSLException e) {
                throw new IgniteException(e);
            }
        }

        return ctx;
    }

    public interface TrustManagersFactory {
        TrustManager[] create(String keyAlgorithm) throws SSLException;
    }

    public interface KeyManagersFactory {
        KeyManager[] create(String keyAlgorithm) throws SSLException;
    }

    /** */
    public abstract static class AbstractTrustManagersFactory implements TrustManagersFactory {
        @Override
        public TrustManager[] create(String keyAlgorithm) throws SSLException {
            try {
                TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(keyAlgorithm);

                KeyStore trustStore = loadKeystore();

                trustMgrFactory.init(trustStore);

                return trustMgrFactory.getTrustManagers();
            }
            catch (NoSuchAlgorithmException e) {
                throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
            }
            catch (GeneralSecurityException e) {
                throw new SSLException("Failed to initialize Trust Manager (security exception occurred) " +
                    "[factory=" + this + ']', e);
            }
        }

        protected abstract KeyStore loadKeystore() throws SSLException;
    }

    /** */
    public abstract static class AbstractKeyManagersFactory implements KeyManagersFactory {
        /** Key store password */
        protected char[] keyStorePwd;

        @Override
        public KeyManager[] create(String keyAlgorithm) throws SSLException {
            try {
                KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(keyAlgorithm);

                KeyStore keyStore = loadKeystore();

                keyMgrFactory.init(keyStore, keyStorePwd);

                return keyMgrFactory.getKeyManagers();
            }
            catch (NoSuchAlgorithmException e) {
                throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
            }
            catch (GeneralSecurityException e) {
                throw new SSLException("Failed to initialize Key Manager (security exception occurred) " +
                    "[factory=" + this + ']', e);
            }
        }

        /**
         * Gets key store password.
         *
         * @return Key store password.
         */
        public char[] getKeyStorePassword() {
            return keyStorePwd;
        }

        /**
         * Sets key store password.
         *
         * @param keyStorePwd Key store password.
         */
        public void setKeyStorePassword(char[] keyStorePwd) {
            A.notNull(keyStorePwd, "keyStorePwd");

            this.keyStorePwd = keyStorePwd;
        }

        protected abstract KeyStore loadKeystore() throws SSLException;
    }
}
