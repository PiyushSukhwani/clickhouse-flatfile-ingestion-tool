const FlatFileConfig = () => {
  return (
    <div
      id="flatfileSourceConfig"
      className="mb-4 border border-gray-400 rounded shadow"
    >
      <div className="bg-blue-600 text-white px-4 py-2 rounded-t">
        <h5 className="mb-0 text-lg font-semibold">
          Step 2: Configure Flat File Source
        </h5>
      </div>
      <div className="p-4 bg-white rounded-b">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              File Path:
            </label>
            <input
              type="text"
              id="flatfileSourcePath"
              placeholder="/path/to/file.csv"
              className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              Delimiter:
            </label>
            <input
              type="text"
              id="flatfileSourceDelimiter"
              defaultValue=","
              placeholder=","
              className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="flatfileSourceHasHeader"
              defaultChecked
              className="w-4 h-4 text-blue-600 border-gray-400 focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
            <label
              htmlFor="flatfileSourceHasHeader"
              className="text-md text-gray-700"
            >
              File has header row
            </label>
          </div>
          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              Encoding:
            </label>
            <input
              type="text"
              id="flatfileSourceEncoding"
              defaultValue="UTF-8"
              placeholder="UTF-8"
              className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        <div>
          <button
            id="loadFlatfileColumns"
            className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700 transition"
          >
            Load Columns
          </button>
        </div>
      </div>
    </div>
  );
};

export default FlatFileConfig;
