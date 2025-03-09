using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FluentAssertions;

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

    }

}
