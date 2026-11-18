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
// Copyright 2026 qjwu. Part of the multi-identity extensions of this fork (see README.md, "Modifications").
package org.apache.kafka.common.security.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A certificate store in folders protected by the operating system (Linux, macOS): no passwords, the files are readable
 * only by the Kafka process user, and the OS audits access (e.g. {@code auditd} rules, configured outside Kafka).
 * <ul>
 *   <li><b>Trust folder</b>: CA certificates to trust, in PEM files ({@code *.pem}, {@code *.crt}); a file may hold
 *       several certificates. Every certificate is a separate trust anchor.</li>
 *   <li><b>Key folder</b> (optional): {@code chain.pem} — the own certificate first, then its issuers (e.g. intermediate,
 *       root), sent to the peer during the handshake — and {@code key.pem} — the private key, unencrypted PKCS#8
 *       ({@code -----BEGIN PRIVATE KEY-----}).</li>
 * </ul>
 * With the permission check on (default), a folder or file that is not owned by the process user or that grants any
 * permission to group or others is refused, and so is a symbolic link: then nothing is loaded.
 */
public class DirectoryCertificateStore implements OsCertificateStore {
    public static final String CHAIN_FILE = "chain.pem";
    public static final String KEY_FILE = "key.pem";
    private static final Logger log = LoggerFactory.getLogger(DirectoryCertificateStore.class);
    private static final Pattern PRIVATE_KEY_PEM = Pattern.compile(
        "-----BEGIN PRIVATE KEY-----([A-Za-z0-9+/=\\s]+)-----END PRIVATE KEY-----");
    private static final Set<PosixFilePermission> GROUP_OR_OTHERS = EnumSet.of(
        PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
        PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

    private final Path trustDir;
    private final Path keyDir;
    private final boolean permissionCheck;

    /**
     * @param trustDir folder with the CA certificates to trust
     * @param keyDir folder with {@code chain.pem} and {@code key.pem}, or null for a trust-only store
     * @param permissionCheck whether to refuse folders / files that are too open (POSIX file systems only)
     */
    public DirectoryCertificateStore(Path trustDir, Path keyDir, boolean permissionCheck) {
        this.trustDir = trustDir;
        this.keyDir = keyDir;
        this.permissionCheck = permissionCheck;
    }

    @Override
    public Material load() throws IOException, GeneralSecurityException {
        List<TrustedCa> cas = new ArrayList<>();
        for (Path file : certificateFiles(trustDir)) {
            for (X509Certificate certificate : readCertificates(file))
                cas.add(new TrustedCa(trustDir.relativize(file).toString(), certificate));
        }
        if (cas.isEmpty())
            throw new GeneralSecurityException("No CA certificate in " + trustDir);
        if (keyDir == null)
            return new Material(cas, null, new ArrayList<>());
        checkProtected(keyDir, true);
        Path chainFile = keyDir.resolve(CHAIN_FILE);
        Path keyFile = keyDir.resolve(KEY_FILE);
        checkProtected(chainFile, false);
        checkProtected(keyFile, false);
        List<X509Certificate> chain = readCertificates(chainFile);
        if (chain.isEmpty())
            throw new GeneralSecurityException("No certificate in " + chainFile);
        PrivateKey key = readPrivateKey(keyFile, chain.get(0).getPublicKey().getAlgorithm());
        return new Material(cas, key, chain);
    }

    @Override
    public String describe() {
        return "folders trust=" + trustDir + (keyDir != null ? ", key=" + keyDir : "");
    }

    private List<Path> certificateFiles(Path dir) throws IOException {
        checkProtected(dir, true);
        try (Stream<Path> files = Files.list(dir)) {
            List<Path> result = files.filter(p -> {
                String name = p.getFileName().toString();
                return (name.endsWith(".pem") || name.endsWith(".crt")) && !name.startsWith(".");
            }).sorted().collect(Collectors.toList());
            for (Path file : result)
                checkProtected(file, false);
            return result;
        }
    }

    /**
     * Refuse a path that isn't a regular folder / file owned by the process user with no group / others permission.
     *
     * @param path folder or file
     * @param directory whether it must be a folder
     * @throws IOException if the check fails
     */
    void checkProtected(Path path, boolean directory) throws IOException {
        if (Files.isSymbolicLink(path))
            throw new IOException(path + " is a symbolic link; certificate store paths must be real folders / files");
        if (directory ? !Files.isDirectory(path) : !Files.isRegularFile(path))
            throw new IOException(path + (directory ? " is not a folder" : " is not a regular file"));
        if (!permissionCheck)
            return;
        PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (view == null) {
            log.warn("No POSIX permissions on {}; permission check skipped", path);
            return;
        }
        String owner = view.readAttributes().owner().getName();
        String processUser = System.getProperty("user.name");
        if (!owner.equals(processUser))
            throw new IOException(String.format("%s is owned by '%s', not by the process user '%s'", path, owner, processUser));
        Set<PosixFilePermission> open = EnumSet.copyOf(view.readAttributes().permissions());
        open.retainAll(GROUP_OR_OTHERS);
        if (!open.isEmpty())
            throw new IOException(String.format("%s grants %s; only the owner may have access (e.g. chmod %s)", path,
                open, directory ? "700" : "600"));
    }

    static List<X509Certificate> readCertificates(Path file) throws IOException, GeneralSecurityException {
        byte[] content = Files.readAllBytes(file);
        if (!new String(content, StandardCharsets.US_ASCII).contains("-----BEGIN CERTIFICATE-----"))
            return new ArrayList<>();
        Collection<? extends Certificate> certificates =
            CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(content));
        List<X509Certificate> result = new ArrayList<>();
        for (Certificate certificate : certificates)
            result.add((X509Certificate) certificate);
        return result;
    }

    static PrivateKey readPrivateKey(Path file, String algorithm) throws IOException, GeneralSecurityException {
        String pem = new String(Files.readAllBytes(file), StandardCharsets.US_ASCII);
        if (pem.contains("ENCRYPTED PRIVATE KEY"))
            throw new GeneralSecurityException(file + ": encrypted keys are not supported; the key is protected by the " +
                "folder permissions (convert with: openssl pkcs8 -topk8 -nocrypt)");
        Matcher matcher = PRIVATE_KEY_PEM.matcher(pem);
        if (!matcher.find())
            throw new GeneralSecurityException(file + ": no '-----BEGIN PRIVATE KEY-----' (PKCS#8) block; convert a " +
                "PKCS#1 key with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem");
        byte[] der = Base64.getMimeDecoder().decode(matcher.group(1));
        try {
            return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (InvalidKeySpecException e) {
            throw new GeneralSecurityException(file + ": the private key doesn't match the certificate's key algorithm " + algorithm, e);
        }
    }
}
