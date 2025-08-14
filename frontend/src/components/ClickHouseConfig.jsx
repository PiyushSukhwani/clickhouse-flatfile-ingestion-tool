import { useEffect, useRef, useState } from "react";
import {
  fetchClickhouseSchema,
  fetchClickhouseTables,
  testClickhouseConnection,
} from "../services/clickhouseService";

const ClickHouseConfig = ({ selectionType, setColumns, formDataRef }) => {
  const [showJoinConfig, setShowJoinConfig] = useState(false);
  const [connectionLoading, setConnectionLoading] = useState(false);
  const [tablesLoading, setTablesLoading] = useState(false);
  const bottomRef = useRef(null);
  const [error, setError] = useState(true);
  const [message, setMessage] = useState("");
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState("");

  const [formData, setFormData] = useState({
    host: "",
    port: 8123,
    database: "default",
    user: "default",
    jwtToken: "",
    secure: false,
  });

  const handleFormChange = (e) => {
    const { id, value } = e.target;

    const updatedFormData = {
      ...formData,
      [id]: id === "port" ? Number(value) : value,
    };

    setFormData(updatedFormData);
    formDataRef.current.set(
      "clickHouseConfig",
      JSON.stringify(updatedFormData)
    );
  };

  const testConnection = async () => {
    setMessage("");
    setConnectionLoading(true);
    setTables([]);
    try {
      const res = await testClickhouseConnection(formData);
      setMessage(res.message);
      setError(false);
    } catch (error) {
      setError(true);
      setMessage("Connection failed.");
    } finally {
      setConnectionLoading(false);
    }
  };

  const fetchTables = async () => {
    setMessage("");
    setTablesLoading(true);
    setTables([]);
    formDataRef.current.set("tableName", selectedTable)
    try {
      const res = await fetchClickhouseTables(formData);
      setMessage("Tables fetched successfully.");
      setError(false);
      setTables(res.tables);
    } catch (error) {
      setError(true);
      setMessage("Error fetching Tables.");
    } finally {
      setTablesLoading(false);
    }
  };

  const fetchSchema = async () => {
    formDataRef.current.set("tableName", selectedTable);
    try {
      const res = await fetchClickhouseSchema(formData, selectedTable);
      setColumns?.(res?.columns);
    } catch (error) {
      console.error(error);
    }
  };

  useEffect(() => {
    if (tables.length > 0) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [tables]);

  return (
    <div
      className="bg-white shadow rounded-lg mb-6 border border-gray-300"
      id="clickhouseSourceConfig"
    >
      <div className="bg-blue-500 text-white px-6 py-3 rounded-t-lg">
        <h5 className="text-lg font-medium">
          Step 2: Configure ClickHouse{" "}
          {selectionType.charAt(0).toUpperCase() + selectionType.slice(1)}
        </h5>
      </div>
      <div className="px-6 py-4 ">
        {/* Host & Port */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          <div>
            <label
              htmlFor="clickhouseSourceHost"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              Host:
            </label>
            <input
              type="text"
              id="host"
              onChange={(e) => handleFormChange(e)}
              placeholder="localhost"
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
          <div>
            <label
              htmlFor="clickhouseSourcePort"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              Port:
            </label>
            <input
              type="number"
              id="port"
              defaultValue={8123}
              onChange={(e) => handleFormChange(e)}
              placeholder="Port"
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        {/* Database & User */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          <div>
            <label
              htmlFor="clickhouseSourceDatabase"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              Database:
            </label>
            <input
              type="text"
              id="database"
              placeholder="Database"
              defaultValue={"default"}
              onChange={(e) => handleFormChange(e)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
          <div>
            <label
              htmlFor="clickhouseSourceUser"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              User:
            </label>
            <input
              type="text"
              id="user"
              defaultValue={"default"}
              placeholder="User"
              onChange={(e) => handleFormChange(e)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        {/* JWT & Connection Type */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          <div>
            <label
              htmlFor="clickhouseSourceJwtToken"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              JWT Token:
            </label>
            <input
              type="password"
              id="jwtToken"
              onChange={(e) => handleFormChange(e)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
          <div>
            <label
              htmlFor="clickhouseSourceSecure"
              className="block text-sm font-medium mb-1 text-gray-800"
            >
              Connection Type:
            </label>
            <select
              id="secure"
              onChange={(e) => handleFormChange(e)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            >
              <option value="false">HTTP (8123/9000)</option>
              <option value="true">HTTPS (8443/9440)</option>
            </select>
          </div>
        </div>

        {/* Test & Load Buttons */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          {selectionType === "source" ? (
            <div className="col-span-2 flex gap-4">
              <button
                id="testClickhouseConnection"
                onClick={testConnection}
                disabled={connectionLoading || tablesLoading}
                className={`w-full md:w-auto px-4 py-2 rounded text-white flex items-center justify-center gap-2
    ${
      connectionLoading || tablesLoading
        ? "bg-blue-400 cursor-not-allowed"
        : "bg-blue-600 hover:bg-blue-700"
    }
  `}
              >
                {connectionLoading ? (
                  <>
                    <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    Testing...
                  </>
                ) : (
                  "Test Connection"
                )}
              </button>
              <button
                onClick={fetchTables}
                id="loadClickhouseTables"
                disabled={connectionLoading || tablesLoading}
                className={`w-full md:w-auto  text-white px-4 py-2 rounded flex items-center justify-center gap-2 ${
                  connectionLoading || tablesLoading
                    ? "cursor-not-allowed bg-green-400"
                    : "bg-green-600 hover:bg-green-700"
                }`}
              >
                {tablesLoading ? (
                  <>
                    <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    Loading Tables...
                  </>
                ) : (
                  "Load Tables"
                )}
              </button>
            </div>
          ) : (
            <>
              <div>
                <label
                  htmlFor="clickhouseTableName"
                  className="block text-sm font-medium mb-1 text-gray-800"
                >
                  Target Table Name:
                </label>
                <input
                  type="text"
                  onChange={(e) => formDataRef.current.set("targetTableName", e.target.value)}
                  id="clickhouseTableName"
                  placeholder="target_table"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
                />
              </div>
              <div className="flex items-end">
                <button
                  id="testClickhouseConnection"
                  onClick={testConnection}
                  className={`w-full md:w-auto px-4 py-2 rounded text-white flex items-center justify-center gap-2
    ${
      connectionLoading
        ? "bg-blue-400 cursor-not-allowed"
        : "bg-blue-600 hover:bg-blue-700"
    }
  `}
                >
                  {connectionLoading ? (
                    <>
                      <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                      Testing...
                    </>
                  ) : (
                    "Test Connection"
                  )}
                </button>
              </div>
            </>
          )}
        </div>

        {/* Connection Result */}
        {message && (
          <div
            ref={bottomRef}
            id="clickhouseConnectionResult"
            className={`my-6 px-6 py-5 rounded-lg  border ${
              error
                ? "text-red-800 bg-red-100 border-red-300"
                : "text-green-800  border-green-300 bg-green-100"
            }`}
          >
            {message}
          </div>
        )}

        {/* Table Selection */}
        {tables?.length > 0 && (
          <div id="clickhouseTableSelection" className="mt-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
              <div>
                <label
                  htmlFor="clickhouseTableList"
                  className="block text-md font-medium mb-2 text-gray-800"
                >
                  Select Table:
                </label>
                <select
                  id="clickhouseTableList"
                  onChange={(e) => setSelectedTable(e.target.value)}
                  className="w-full border rounded-md px-3 py-2 border-gray-300 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
                >
                  <option value="">-- Select Table --</option>
                  {tables?.map((table, idx) => (
                    <option value={table} key={idx}>
                      {table}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex items-end">
                <button
                  id="loadClickhouseColumns"
                  onClick={fetchSchema}
                  className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
                >
                  Load Columns
                </button>
              </div>
            </div>

            {/* Multi-Table Join */}
            <div className="mb-4 hidden">
              <label className="inline-flex items-center">
                <input
                  type="checkbox"
                  id="enableJoin"
                  className="mr-2"
                  onChange={(e) => setShowJoinConfig(e.target.checked)}
                />
                Enable Multi-Table Join (Bonus Feature)
              </label>
            </div>

            {/* Join Configuration */}
            {showJoinConfig && (
              <div id="joinConfiguration" className="hidden">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
                  <div>
                    <label
                      htmlFor="additionalTables"
                      className="block text-sm font-medium mb-1 text-gray-800"
                    >
                      Additional Tables:
                    </label>
                    <select
                      id="additionalTables"
                      multiple
                      size={3}
                      className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
                    />
                  </div>
                  <div>
                    <label
                      htmlFor="joinCondition"
                      className="block text-sm font-medium mb-1 text-gray-800"
                    >
                      Join Condition:
                    </label>
                    <input
                      type="text"
                      id="joinCondition"
                      placeholder="table1.id = table2.id"
                      className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ClickHouseConfig;
