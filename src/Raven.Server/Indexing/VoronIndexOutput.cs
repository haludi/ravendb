﻿using System;
using System.IO;
using Lucene.Net.Store;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron.Impl;
using Voron;

namespace Raven.Server.Indexing
{
    public class VoronIndexOutput : BufferedIndexOutput
    {
        public static readonly int MaxFileChunkSize = 128 * 1024 * 1024;

        private readonly string _name;
        private readonly string _tree;
        private readonly Transaction _tx;
        private readonly Stream _file;
        private readonly string _fileTempPath;

        public VoronIndexOutput(StorageEnvironmentOptions options, string name, Transaction tx, string tree)
        {
            _name = name;
            _tree = tree;
            _tx = tx;
            _fileTempPath =ne(name + "_" + Guid.NewGuid()).FullPath;

            if (options.EncryptionEnabled)
                _file = new TempCryptoStream(_fileTempPath);
            else
                _file = SafeFileStream.Create(_fileTempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.DeleteOnClose);

            _tx.ReadTree(_tree).AddStream(name, Stream.Null); // ensure it's visible by LuceneVoronDirectory.FileExists, the actual write is inside Dispose
        }

        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            _file.Write(b, offset, len);
        }

        /// <summary>Random-access methods </summary>
        public override void Seek(long pos)
        {
            base.Seek(pos);
            _file.Seek(pos, SeekOrigin.Begin);
        }

        public override long Length => _file.Length;

        public override void SetLength(long length)
        {
            _file.SetLength(length);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            
            var files = _tx.ReadTree(_tree);

            using (Slice.From(_tx.Allocator, _name, out Slice nameSlice))
            {
                _file.Seek(0, SeekOrigin.Begin);
                files.AddStream(nameSlice, _file);
            }
            
            _file.Dispose();
            PosixFile.DeleteOnClose(_fileTempPath);
        }
    }
}
