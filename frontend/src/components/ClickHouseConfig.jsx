import { useState } from "react";

const ClickHouseConfig = ({ selectionType = "target" }) => {
  const [showJoinConfig, setShowJoinConfig] = useState(false);

  return (
    <div
      className="bg-white shadow rounded-lg mb-6 border border-gray-300"
      id="clickhouseSourceConfig"
    >
      <div className="bg-blue-500 text-white px-6 py-3 rounded-t-lg">
        <h5 className="text-lg font-medium">
          Step 2: Configure ClickHouse Source
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
              id="clickhouseSourceHost"
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
              id="clickhouseSourcePort"
              placeholder="8123"
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
              id="clickhouseSourceDatabase"
              placeholder="default"
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
              id="clickhouseSourceUser"
              placeholder="default"
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
              id="clickhouseSourceJwtToken"
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
              id="clickhouseSourceSecure"
              className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            >
              <option value="false">HTTP (8123/9000)</option>
              <option value="true">HTTPS (8443/9440)</option>
            </select>
          </div>
        </div>

        {/* Test & Load Buttons */}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          {selectionType === "target" ? (
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
                  id="clickhouseTableName"
                  placeholder="target_table"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
                />
              </div>
              <div className="flex items-end">
                <button
                  id="testClickhouseConnection"
                  className="w-full md:w-auto bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
                >
                  Test Connection
                </button>
              </div>
            </>
          ) : (
            <div className="col-span-2 flex gap-4">
              <button
                id="testClickhouseConnection"
                className="w-full md:w-auto bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
              >
                Test Connection
              </button>
              <button
                id="loadClickhouseTables"
                className="w-full md:w-auto bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
              >
                Load Tables
              </button>
            </div>
          )}
        </div>

        {/* Connection Result */}
        <div
          id="clickhouseConnectionResult"
          className="my-6 px-6 py-5 rounded-lg bg-blue-100 text-green-900"
        >
          Hi. How are you{" "}
        </div>

        {/* Table Selection */}
        <div id="clickhouseTableSelection" className="mt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
            <div>
              <label
                htmlFor="clickhouseTableList"
                className="block text-sm font-medium mb-1 text-gray-800"
              >
                Select Table:
              </label>
              <select
                id="clickhouseTableList"
                className="w-full border rounded-md px-3 py-2 border-gray-300 focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
              />
            </div>
            <div className="flex items-end">
              <button
                id="loadClickhouseColumns"
                className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
              >
                Load Columns
              </button>
            </div>
          </div>

          {/* Multi-Table Join */}
          <div className="mb-4">
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
            <div id="joinConfiguration">
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
      </div>
    </div>
  );
};

export default ClickHouseConfig;
