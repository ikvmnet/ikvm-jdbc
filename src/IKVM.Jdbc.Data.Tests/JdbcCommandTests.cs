using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using com.sun.org.apache.bcel.@internal.util;

using FluentAssertions;

using IKVM.Jdbc.Data.Internal;

using java.awt.color;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcCommandTests
    {

        JdbcConnection CreateTestConnection()
        {
            return new JdbcConnection(org.sqlite.JDBC.createConnection("jdbc:sqlite:sample.db", new java.util.Properties()));
        }

        [TestMethod]
        public void CanGetColumnNames()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetColumnNames";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetColumnNames (id integer, name string)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetColumnNames";
            using var rdr = cmd.ExecuteReader();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("name");
        }

        [TestMethod]
        public void CanGetBytesToArray()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToArray";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToArray (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToArray values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToArray";
            using var rdr = cmd.ExecuteReader();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = new byte[32];
            rdr.GetBytes(1, 0, buffer, 0, buffer.Length).Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
            for (int i = 16; i < 32; i++)
                buffer[i].Should().Be((byte)0);
        }

        [TestMethod]
        public void CanGetBytesToSpan()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToSpan";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToSpan (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToSpan values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToSpan";
            using var rdr = cmd.ExecuteReader();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = (Span<byte>)stackalloc byte[32];
            rdr.GetBytes(1, 0, buffer).Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
            for (int i = 16; i < 32; i++)
                buffer[i].Should().Be((byte)0);
        }

        [TestMethod]
        public void CanGetBytesToStream()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToStream";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToStream (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToStream values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToStream";
            using var rdr = cmd.ExecuteReader();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            using var stm = rdr.GetStream(1);
            var buffer = stm.ReadAllBytes(4);
            buffer.Length.Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
        }

    }

}
