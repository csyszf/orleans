using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    internal static class GrainIdExtensions
    {
        public static GrainId FromByteArray(byte[] byteArray)
        {
            var reader = new BinaryTokenStreamReader(byteArray);
            return reader.ReadGrainId();
        }

        public static byte[] ToByteArray(this GrainId @this)
        {
            var output = new ByteArrayBufferWriter();
            var writer = new BinaryTokenStreamWriterV2(output);
            writer.Write(@this);
            var result = output.Buffer.ToArray();
            output.ReleaseBuffers();
            return result;
        }

        public static void Serialize(this GuidId @this, BinaryTokenStreamWriterV2 writer)
        {
            writer.Write(@this.Guid);
        }
    }
}
