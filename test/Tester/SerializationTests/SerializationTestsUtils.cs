using System.Buffers;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Xunit;

namespace Tester.SerializationTests
{
    public class SerializationTestsUtils
    {
        public static void VerifyUsingFallbackSerializer(SerializationManager serializationManager, object ob)
        {
            var output = new ByteArrayBufferWriter();
            var context = new SerializationContext(serializationManager, output);
            var writer = new BinaryTokenStreamWriter(context);
            serializationManager.FallbackSerializer(ob, ref writer, ob.GetType());
            var bytes = output.Buffer.ToArray();

            var reader = new BinaryTokenStreamReader(bytes);
            var serToken = reader.ReadToken();
            Assert.Equal(SerializationTokenType.Fallback, serToken);
        }
    }
}
