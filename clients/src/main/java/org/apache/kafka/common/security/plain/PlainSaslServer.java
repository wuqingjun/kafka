/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.plain;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.SaslServerCallbackHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import net.sf.jni4net.Bridge;

import dstsauthentication.DstsAuthentication;
import dstsauthentication.AuthenticationResult;
import dstsauthentication.Claim;
import org.apache.zookeeper.AsyncCallback.*;//{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.*;//{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.*;//{ACL, Stat}
import org.apache.zookeeper.*;//{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

/**
 * Simple SaslServer implementation for SASL/PLAIN. In order to make this implementation
 * fully pluggable, authentication of username/password is fully contained within the
 * server implementation.
 * <p>
 * Valid users with passwords are specified in the Jaas configuration file. Each user
 * is specified with user_<username> as key and <password> as value. This is consistent
 * with Zookeeper Digest-MD5 implementation.
 * <p>
 * To avoid storing clear passwords on disk or to integrate with external authentication
 * servers in production systems, this module can be replaced with a different implementation.
 *
 */
public class PlainSaslServer implements SaslServer {
    private static final Logger log = LoggerFactory.getLogger(PlainSaslServer.class);

    public static final String PLAIN_MECHANISM = "PLAIN";
    private static final String JAAS_USER_PREFIX = "user_";

    public static final String LIB_PATH = "libs";
    public static final String NAME_IDENTIFIER="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier";
    public static final String DSTS_SERVICEDNSNAME="dsts.serviceDnsName";
    public static final String DSTS_SERVICENAME="dsts.serviceName";
    public static final String DSTS_DSTSREALM="dsts.dstsRealm";
    public static final String DSTS_DSTSDNSNAME="dsts.dstsDnsName";
    public static final String DSTS_AUTHENTICATION_J4N_LIBRARY = "dsts.authenticationJ4nLibrary";
    public static final String AUTHENTICATION_STATUS_OK="OK";

    private final JaasContext jaasContext;
    private final Map<String, ?> props;

    private boolean complete;
    private String authorizationId;

    public PlainSaslServer(JaasContext jaasContext, Map<String, ?> props) {
        this.jaasContext = jaasContext;
        this.props = props;
    }

    /**
     * @throws SaslAuthenticationException if username/password combination is invalid or if the requested
     *         authorization id is not the same as username.
     * <p>
     * <b>Note:</b> This method may throw {@link SaslAuthenticationException} to provide custom error messages
     * to clients. But care should be taken to avoid including any information in the exception message that
     * should not be leaked to unauthenticated clients. It may be safer to throw {@link SaslException} in
     * some cases so that a standard error message is returned to clients.
     * </p>
     */
    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException, SaslAuthenticationException {
        /*
         * Message format (from https://tools.ietf.org/html/rfc4616):
         *
         * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
         * authcid   = 1*SAFE ; MUST accept up to 255 octets
         * authzid   = 1*SAFE ; MUST accept up to 255 octets
         * passwd    = 1*SAFE ; MUST accept up to 255 octets
         * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
         *
         * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
         *                ;; any UTF-8 encoded Unicode character except NUL
         */

        String[] tokens;
        try {
            tokens = new String(response, "UTF-8").split("\u0000");
        } catch (UnsupportedEncodingException e) {
            throw new SaslException("UTF-8 encoding not supported", e);
        }
        if (tokens.length != 3)
            throw new SaslException("Invalid SASL/PLAIN response: expected 3 tokens, got " + tokens.length);
        String authorizationIdFromClient = tokens[0];
        String username = tokens[1];
        String password = tokens[2];

        if (username.isEmpty()) {
            throw new SaslException("Authentication failed: username not specified");
        }
        if (password.isEmpty()) {
            throw new SaslException("Authentication failed: password not specified");
        }

        String expectedPassword = jaasContext.configEntryOption(JAAS_USER_PREFIX + username,
                PlainLoginModule.class.getName());

        if(password.equals(expectedPassword)){
            this.authorizationId = username;
            complete = true;
            return new byte[0];
        }

        String j4nLibFilePath = LIB_PATH + "/" + System.getProperty(DSTS_AUTHENTICATION_J4N_LIBRARY);

        try {
            ZooKeeper zk = new ZooKeeper(props.get("zookeeper.connect").toString(), Integer.parseInt(props.get("zookeeper.session.timeout.ms").toString()), new Watcher(){
                public void process(WatchedEvent watchedEvent){
                    log.info("Event state: {}", watchedEvent.getState());
                }
            });

            byte[] bn = zk.getData("/AzPubSubRegistrar/DstsMetadata", false, null);

            String dstsDns = new String(bn,
                    "UTF-8");
            zk.close();

            Bridge.init(new File(LIB_PATH));
            Bridge.LoadAndRegisterAssemblyFrom(new java.io.File(j4nLibFilePath));
            DstsAuthentication authentication = new DstsAuthentication();

            AuthenticationResult res = authentication.Authenticate(System.getProperty(DSTS_DSTSREALM),
                    System.getProperty(DSTS_DSTSDNSNAME),
                    System.getProperty(DSTS_SERVICEDNSNAME),
                    System.getProperty(DSTS_SERVICENAME),
                    password);
            String status = res.getStatus();
            String errorMessage = res.getErrorMessage();

            if(status.equals(AUTHENTICATION_STATUS_OK)) {
                for (Claim claim : res.getClaims()) {
                    if(claim.getClaimType().equals(NAME_IDENTIFIER)){
                        String clientId = claim.getValue();
                        if(null != clientId && !clientId.isEmpty()){
                            log.info("Authorzied Id from SAML token: {}", clientId);
                            this.authorizationId = clientId;
                            complete = true;
                            return new byte[0];
                        }
                    }
                }
            }
            String error = String.format("Failed to authenticate token for user: {}, status: {}, error message: {}", username, status, errorMessage);
            log.error(error);
            throw new SaslAuthenticationException(error);
        }
        catch(FileNotFoundException e) {
            String error = String.format("Authentication J4n Assembly cannot be found under folder: {}, error message: {}, cause: {}", LIB_PATH, e.getMessage(), e.getCause().getMessage());
            log.error(error);
            throw new SaslException(error);
        }
        catch(UnsupportedClassVersionError e) {
            String error = String.format("Class Version not supported error: {}", e.getMessage());
            log.error(error);
            throw new SaslException(error);
        }
        catch(UnsatisfiedLinkError e) {
            String error = String.format("JNI error: unsatisfied link error: {}", e.getMessage());
            log.error(error);
            throw new SaslException(error);
        }
        catch(Exception e){
            String error = String.format("Unknown exception happened in Saml Authentication Provider: {}, cause: {}", e.getMessage(), e.getCause().getMessage());
            log.error(error);
            throw new SaslException(error);
        }
    }

    @Override
    public String getAuthorizationID() {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return authorizationId;
    }

    @Override
    public String getMechanismName() {
        return PLAIN_MECHANISM;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return null;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public void dispose() throws SaslException {
    }

    public static class PlainSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh)
            throws SaslException {

            if (!PLAIN_MECHANISM.equals(mechanism))
                throw new SaslException(String.format("Mechanism \'%s\' is not supported. Only PLAIN is supported.", mechanism));

            if (!(cbh instanceof SaslServerCallbackHandler))
                throw new SaslException("CallbackHandler must be of type SaslServerCallbackHandler, but it is: " + cbh.getClass());

            return new PlainSaslServer(((SaslServerCallbackHandler) cbh).jaasContext(), props);
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            if (props == null) return new String[]{PLAIN_MECHANISM};
            String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
            if ("true".equals(noPlainText))
                return new String[]{};
            else
                return new String[]{PLAIN_MECHANISM};
        }
    }
}
