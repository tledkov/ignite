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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * SSL context factory that provides SSL context configuration with specified key and trust stores.
 *
 * This factory caches the result of the first successful attempt to create an {@link SSLContext} and always returns it
 * as a result of further invocations of the {@link SslContextFactory#create()}} method.
 * <p>
 * In some cases it is useful to disable certificate validation of client side (e.g. when connecting
 * to a server with self-signed certificate). This can be achieved by setting a disabled trust manager
 * to this factory, which can be obtained by {@link #getDisabledTrustManager()} method:
 * <pre>
 *     SslContextFactory factory = new SslContextFactory();
 *     factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
 *     // Rest of initialization.
 * </pre>
 */
public class SslContextFactory extends AbstractSslContextFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default key / trust store type. */
    public static final String DFLT_STORE_TYPE = AbstractSslContextFactory.DFLT_STORE_TYPE;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = AbstractSslContextFactory.DFLT_SSL_PROTOCOL;

    /** Default key manager / trust manager algorithm. Specifying different trust manager algorithm is not supported. */
    public static final String DFLT_KEY_ALGORITHM = AbstractSslContextFactory.DFLT_KEY_ALGORITHM;

    /** */
    public SslContextFactory() {
        super(new FileBasedKeyManagersFactory(), new FileBasedTrustManagersFactory());
    }

    /**
     * Gets key store type used for context creation.
     *
     * @return Key store type.
     */
    public String getKeyStoreType() {
        return ((FileBasedKeyManagersFactory) keyManagersFactory).getKeyStoreType();
    }

    /**
     * Sets key store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
     * be used.
     *
     * @param keyStoreType Key store type.
     */
    public void setKeyStoreType(String keyStoreType) {
        A.notNull(keyStoreType, "keyStoreType");

        ((FileBasedKeyManagersFactory) keyManagersFactory).setKeyStoreType(keyStoreType);
    }

    /**
     * Gets path to the key store file.
     *
     * @return Path to key store file.
     */
    public String getKeyStoreFilePath() {
        return ((FileBasedKeyManagersFactory) keyManagersFactory).getKeyStoreFilePath();
    }

    /**
     * Sets path to the key store file. This is a mandatory parameter since
     * ssl context could not be initialized without key manager.
     *
     * @param keyStoreFilePath Path to key store file.
     */
    public void setKeyStoreFilePath(String keyStoreFilePath) {
        A.notNull(keyStoreFilePath, "keyStoreFilePath");

        ((FileBasedKeyManagersFactory) keyManagersFactory).setKeyStoreFilePath(keyStoreFilePath);
    }

    /**
     * Gets key store password.
     *
     * @return Key store password.
     */
    public char[] getKeyStorePassword() {
        return ((AbstractKeyManagersFactory) keyManagersFactory).getKeyStorePassword();
    }

    /**
     * Sets key store password.
     *
     * @param keyStorePwd Key store password.
     */
    public void setKeyStorePassword(char[] keyStorePwd) {
        A.notNull(keyStorePwd, "keyStorePwd");

        ((AbstractKeyManagersFactory) keyManagersFactory).setKeyStorePassword(keyStorePwd);
    }

    /**
     * Gets trust store type used for context creation.
     *
     * @return trust store type.
     */
    public String getTrustStoreType() {
        return ((FileBasedTrustManagersFactory) trustManagersFactory).getTrustStoreType();
    }

    /**
     * Sets trust store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
     * be used.
     *
     * @param trustStoreType Trust store type.
     */
    public void setTrustStoreType(String trustStoreType) {
        A.notNull(trustStoreType, "trustStoreType");

        ((FileBasedTrustManagersFactory) trustManagersFactory).setTrustStoreType(trustStoreType);
    }

    /**
     * Gets path to the trust store file.
     *
     * @return Path to the trust store file.
     */
    public String getTrustStoreFilePath() {
        return ((FileBasedTrustManagersFactory) trustManagersFactory).getTrustStoreFilePath();
    }

    /**
     * Sets path to the trust store file. This is an optional parameter,
     * however one of the {@code setTrustStoreFilePath(String)}, {@link #setTrustManagers(TrustManager[])}
     * properties must be set.
     *
     * @param trustStoreFilePath Path to the trust store file.
     */
    public void setTrustStoreFilePath(String trustStoreFilePath) {
        ((FileBasedTrustManagersFactory) trustManagersFactory).setTrustStoreFilePath(trustStoreFilePath);    }

    /**
     * Gets trust store password.
     *
     * @return Trust store password.
     */
    public char[] getTrustStorePassword() {
        return ((FileBasedTrustManagersFactory) trustManagersFactory).getTrustStorePassword();
    }

    /**
     * Sets trust store password.
     *
     * @param trustStorePwd Trust store password.
     */
    public void setTrustStorePassword(char[] trustStorePwd) {
        ((FileBasedTrustManagersFactory) trustManagersFactory).setTrustStorePassword(trustStorePwd);
    }

    /**
     * Returns an instance of trust manager that will always succeed regardless of certificate provided.
     *
     * @return Trust manager instance.
     */
    public static TrustManager getDisabledTrustManager() {
        return new DisabledX509TrustManager();
    }

    /**
     * Builds human-readable string with factory parameters.
     *
     * @return Parameters string.
     */
    private String parameters() {

        // TODO with factories
//        StringBuilder buf = new StringBuilder("[keyStoreType=").append(keyStoreType);
//
//        buf.append(", proto=").append(proto).append(", keyStoreFile=").append(keyStoreFilePath);

//        if (trustMgrs != null)
//            buf.append(", trustMgrs=").append(Arrays.toString(trustMgrs));
//        else
//            buf.append(", trustStoreFile=").append(trustStoreFilePath);
//
//        buf.append(']');

        return "";
    }

    /**
     * Checks that all required parameters are set.
     *
     * @throws SSLException If any of required parameters is missing.
     */
    @Override
    protected void checkParameters() throws SSLException {
        // TODO in factories

//        assert keyStoreType != null;
//        assert proto != null;
//
//        checkNullParameter(keyStoreFilePath, "keyStoreFilePath");
//        checkNullParameter(keyStorePwd, "keyStorePwd");
//
//        if (trustMgrs == null) {
//            if (trustStoreFilePath == null)
//                throw new SSLException("Failed to initialize SSL context (either trustStoreFilePath or " +
//                    "trustManagers must be provided)");
//            else
//                checkNullParameter(trustStorePwd, "trustStorePwd");
//        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + parameters();
    }

    /**
     * Loads key store with configured parameters.
     *
     * @param keyStoreType  Type of key store.
     * @param storeFilePath Path to key store file.
     * @param keyStorePwd   Store password.
     * @return Initialized key store.
     * @throws SSLException If key store could not be initialized.
     */
    @Deprecated
    protected KeyStore loadKeyStore(String keyStoreType, String storeFilePath, char[] keyStorePwd)
        throws SSLException {
        return loadKeyStoreFromFile(keyStoreType, storeFilePath, keyStorePwd);
    }

    /**
     *
     */
    public static class FileBasedKeyManagersFactory extends AbstractKeyManagersFactory {
        /**
         * Key store type.
         */
        protected String keyStoreType = DFLT_STORE_TYPE;

        /**
         * Path to key store file
         */
        protected String keyStoreFilePath;

        /**
         * Key store password
         */
        protected char[] keyStorePwd;

        @Override
        protected KeyStore loadKeystore() throws SSLException {
            return loadKeyStoreFromFile(keyStoreType, keyStoreFilePath, keyStorePwd);
        }

        /**
         * Gets key store type used for context creation.
         *
         * @return Key store type.
         */
        public String getKeyStoreType() {
            return keyStoreType;
        }

        /**
         * Sets key store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
         * be used.
         *
         * @param keyStoreType Key store type.
         */
        public void setKeyStoreType(String keyStoreType) {
            A.notNull(keyStoreType, "keyStoreType");

            this.keyStoreType = keyStoreType;
        }

        /**
         * Gets path to the key store file.
         *
         * @return Path to key store file.
         */
        public String getKeyStoreFilePath() {
            return keyStoreFilePath;
        }

        /**
         * Sets path to the key store file. This is a mandatory parameter since
         * ssl context could not be initialized without key manager.
         *
         * @param keyStoreFilePath Path to key store file.
         */
        public void setKeyStoreFilePath(String keyStoreFilePath) {
            A.notNull(keyStoreFilePath, "keyStoreFilePath");

            this.keyStoreFilePath = keyStoreFilePath;
        }
    }

    public static KeyStore loadKeyStoreFromFile(String keyStoreType, String storeFilePath, char[] keyStorePwd)
        throws SSLException {
        try {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);

            try (InputStream input = new FileInputStream(storeFilePath)) {

                keyStore.load(input, keyStorePwd);

                return keyStore;
            }
        } catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize key store (security exception occurred) [type=" +
                keyStoreType + ", keyStorePath=" + storeFilePath + ']', e);
        } catch (FileNotFoundException e) {
            throw new SSLException("Failed to initialize key store (key store file was not found): [path=" +
                storeFilePath + ", msg=" + e.getMessage() + ']');
        } catch (IOException e) {
            throw new SSLException("Failed to initialize key store (I/O error occurred): " + storeFilePath, e);
        }
    }

    public static class FileBasedTrustManagersFactory extends AbstractTrustManagersFactory {
        /** Trust store type. */
        protected String trustStoreType = DFLT_STORE_TYPE;

        /** Path to trust store.*/
        protected String trustStoreFilePath;

        /** Trust store password */
        protected char[] trustStorePwd;

        @Override
        protected KeyStore loadKeystore() throws SSLException {
            return loadKeyStoreFromFile(getTrustStoreType(), getTrustStoreFilePath(), getTrustStorePassword());
        }

        /**
         * Gets trust store type used for context creation.
         *
         * @return trust store type.
         */
        public String getTrustStoreType() {
            return trustStoreType;
        }

        /**
         * Sets trust store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
         * be used.
         *
         * @param trustStoreType Trust store type.
         */
        public void setTrustStoreType(String trustStoreType) {
            A.notNull(trustStoreType, "trustStoreType");

            this.trustStoreType = trustStoreType;
        }

        /**
         * Gets path to the trust store file.
         *
         * @return Path to the trust store file.
         */
        public String getTrustStoreFilePath() {
            return trustStoreFilePath;
        }

        /**
         * Sets path to the trust store file. This is an optional parameter,
         * however one of the {@code setTrustStoreFilePath(String)}, {@link #setTrustManagers(TrustManager[])}
         * properties must be set.
         *
         * @param trustStoreFilePath Path to the trust store file.
         */
        public void setTrustStoreFilePath(String trustStoreFilePath) {
            this.trustStoreFilePath = trustStoreFilePath;
        }

        /**
         * Gets trust store password.
         *
         * @return Trust store password.
         */
        public char[] getTrustStorePassword() {
            return trustStorePwd;
        }

        /**
         * Sets trust store password.
         *
         * @param trustStorePwd Trust store password.
         */
        public void setTrustStorePassword(char[] trustStorePwd) {
            this.trustStorePwd = trustStorePwd;
        }
    }
}