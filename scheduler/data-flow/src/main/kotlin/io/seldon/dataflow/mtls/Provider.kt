package io.seldon.dataflow.mtls

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.charset.Charset
import java.nio.file.attribute.PosixFilePermission
import java.security.KeyFactory
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import kotlin.io.path.Path
import kotlin.io.path.setPosixFilePermissions
import java.nio.file.Files
import java.util.Base64


// TODO - dynamically reload certificates.  Can KafkaStreams handle this or does it need to pause?
object Provider {
    private const val storeType = "JKS"
    private const val certificateType = "X.509"
    private const val keyType = "RSA"
    private const val keyName = "dataflow-engine-key"

    fun keyStoresFromCertificates(certs: CertificateConfig): KeystoreConfig {
        val (trustStoreLocation, trustStorePassword) = trustStoreFromCACert(certs)
        val (keyStoreLocation, keyStorePassword) = keyStoreFromCerts(certs)
        return KeystoreConfig(
            keyStorePassword = keyStorePassword,
            keyStoreLocation = keyStoreLocation,
            trustStorePassword = trustStorePassword,
            trustStoreLocation = trustStoreLocation,
        )
    }

    private fun trustStoreFromCACert(certPaths: CertificateConfig): Pair<FilePath, KeystorePassword> {
        val password = generatePassword()
        val location = generateLocation()
        val trustStore = KeyStore.getInstance(storeType)
        trustStore.load(null, password.toCharArray())

        certsFromFile(certPaths.brokerCaCertPath)
            .forEach { cert ->
                val subjectName = (cert as X509Certificate).subjectX500Principal.name
                trustStore.setCertificateEntry(
                    subjectName,
                    cert,
                )
            }

        FileOutputStream(location)
            .use { outputLocation ->
                trustStore.store(outputLocation, password.toCharArray())
            }

        return location.absolutePath to password
    }

    private fun certsFromFile(fileName: FilePath): Collection<Certificate> {
        val certFile = File(fileName)
        return FileInputStream(certFile)
            .use { certStream ->
                CertificateFactory
                    .getInstance(certificateType)
                    .generateCertificates(certStream)
            }
    }

    private fun keyStoreFromCerts(certPaths: CertificateConfig): Pair<FilePath, KeystorePassword> {
        val password = generatePassword()
        val location = generateLocation()
        val keyStore = KeyStore.getInstance(storeType)
        keyStore.load(null, password.toCharArray())

        val privateKey = privateKeyFromFile(certPaths.keyPath)
        val certs = certsFromFile(certPaths.certPath)
        val caCerts = certsFromFile(certPaths.caCertPath)
        // TODO - check if CA certs are required as part of the chain.  Docs imply this, but unclear.
        keyStore.setKeyEntry(
            keyName,
            privateKey,
            password.toCharArray(), // No password
            certs.union(caCerts).toTypedArray(),
        )

        FileOutputStream(location)
            .use { outputLocation ->
                keyStore.store(outputLocation, password.toCharArray())
            }

        return location.absolutePath to password
    }

    @Throws(Exception::class)
    fun privateKeyFromFile(filename: FilePath): RSAPrivateKey {
        val file = File(filename)
        val key = String(Files.readAllBytes(file.toPath()), Charset.defaultCharset())
        val privateKeyPEM = key
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replace(System.lineSeparator().toRegex(), "")
            .replace("-----END PRIVATE KEY-----", "")
        val encoded: ByteArray = Base64.getDecoder().decode(privateKeyPEM)
        val keyFactory = KeyFactory.getInstance("RSA")
        val keySpec = PKCS8EncodedKeySpec(encoded)
        return keyFactory.generatePrivate(keySpec) as RSAPrivateKey
    }

    private fun generatePassword(): KeystorePassword {
        return "changeit"
    }

    private fun generateLocation(): File {
        return kotlin.io.path
            .createTempFile(
                directory = Path("/tmp"),
                suffix = ".jks",
            ).setPosixFilePermissions(
                setOf(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                ),
            )
            .toFile()
    }
}