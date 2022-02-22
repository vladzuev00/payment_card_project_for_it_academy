package by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class StringToStringCryptographer implements Cryptographer<String, String>
{
    private final Base64.Encoder encoder;
    private final Base64.Decoder decoder;

    public static StringToStringCryptographer createCryptographer()
    {
        if(StringToStringCryptographer.cryptographer == null)
        {
            synchronized(StringToStringCryptographer.class)
            {
                if(StringToStringCryptographer.cryptographer == null)
                {
                    final Base64.Encoder encoder = Base64.getEncoder();
                    final Base64.Decoder decoder = Base64.getDecoder();
                    StringToStringCryptographer.cryptographer = new StringToStringCryptographer(encoder, decoder);
                }
            }
        }
        return StringToStringCryptographer.cryptographer;
    }

    private static StringToStringCryptographer cryptographer = null;

    private StringToStringCryptographer(final Base64.Encoder encoder, final Base64.Decoder decoder)
    {
        super();
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public final String encrypt(final String encryptedString)
    {
        return this.encoder.encodeToString(encryptedString.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public final String decrypt(final String decryptedData)
    {
        return new String(this.decoder.decode(decryptedData));
    }
}
