using System;
using System.IO;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Run;

namespace PipServices.Blobs.Client.Version1
{
    public class TempBlobStorage : IConfigurable, IOpenable, ICleanable
    {
        private string _path = "./data/temp";
        private long _maxBlobSize = 100 * 1024;
        private long _minChunkSize = 5 * 1024 * 1024;
        private long _cleanupTimeout = 9000000;
        private long _writeTimeout = 9000000;
        private FixedRateTimer _cleanupInterval = null;
        private bool _opened = false;

        public TempBlobStorage(string path = null)
        {
            _path = path;
        }

        public void Configure(ConfigParams config)
        {
            _path = config.GetAsStringWithDefault("temp_path", _path);
            _minChunkSize = config.GetAsLongWithDefault("options.min_chunk_size", _minChunkSize);
            _maxBlobSize = config.GetAsLongWithDefault("options.max_blob_size", _maxBlobSize);
            _cleanupTimeout = config.GetAsLongWithDefault("options.cleanup_timeout", _cleanupTimeout);
            _writeTimeout = config.GetAsLongWithDefault("options.write_timeout", _writeTimeout);
        }

        public bool IsOpen()
        {
            return _opened;
        }

        public async Task OpenAsync(string correlationId)
        {
            if (_opened == true) return;

            // Create filter if it doesn't exist
            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            // Restart cleanup process
            if (_cleanupInterval != null)
                _cleanupInterval.Stop();

            _cleanupInterval = new FixedRateTimer(
                () => { Cleanup(null); },
                (int)_cleanupTimeout, (int)_cleanupTimeout
            );
            _cleanupInterval.Start();

            _opened = true;

            await Task.Delay(0);
        }

        public async Task CloseAsync(string correlationId)
        {
            // Stop cleanup process
            if (_cleanupInterval != null)
            {
                _cleanupInterval.Stop();
                _cleanupInterval = null;
            }

            _opened = false;

            await Task.Delay(0);
        }

        public async Task ClearAsync(string correlationId)
        {
            var files = Directory.GetFiles(_path);

            foreach (var file in files)
            {
                if (file.EndsWith(".dat", StringComparison.InvariantCulture))
                {
                    var filePath = Path.Combine(_path, file);
                    File.Delete(filePath);
                }
            }

            await Task.Delay(0);
        }

        public string MakeFileName(string id)
        {
            return Path.Combine(_path, id + ".tmp");
        }

        public long GetChunksSize(string correlationId, string id)
        {
            var filePath = MakeFileName(id);
            var fileInfo = new FileInfo(filePath);

            // Read temp size
            return fileInfo.Exists ? fileInfo.Length : 0;
        }

        public long AppendChunk(string correlationId, string id, byte[] buffer)
        {
            var size = GetChunksSize(correlationId, id);

            // Enforce max blob size
            size = size + buffer.Length;
            if (size > _maxBlobSize)
            {
                throw new BadRequestException(
                    correlationId,
                    "BLOB_TOO_LARGE",
                    "Blob " + id + " exceeds allowed maximum size of " + _maxBlobSize
                )
                .WithDetails("blob_id", id)
                .WithDetails("size", size)
                .WithDetails("max_size", _maxBlobSize);
            }

            var filePath = MakeFileName(id);
            using (var stream = new FileStream(filePath, FileMode.Append))
            {
                stream.Write(buffer, 0, buffer.Length);
                return stream.Position;
            }
        }

        public byte[] ReadChunks(string correlationId, string id)
        {
            var filePath = MakeFileName(id);
            return File.ReadAllBytes(filePath);
        }

        public void DeleteChunks(string correlationId, string id)
        {
            var filePath = MakeFileName(id);
            if (File.Exists(filePath))
                File.Delete(filePath);
        }

        public void Cleanup(string correlationId)
        {
            var cutoffTime = DateTime.Now.Subtract(TimeSpan.FromMilliseconds(_writeTimeout));

            var files = Directory.GetFiles(_path);

            foreach (var file in files)
            {
                if (file.EndsWith(".dat", StringComparison.InvariantCulture))
                {
                    var filePath = Path.Combine(_path, file);
                    var fileAccessTime = File.GetCreationTime(filePath);
                    if (fileAccessTime < cutoffTime)
                        File.Delete(filePath);
                }
            }
        }

    }
}
