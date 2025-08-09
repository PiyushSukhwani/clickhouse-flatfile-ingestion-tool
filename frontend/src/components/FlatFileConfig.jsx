import { useEffect, useState } from "react";
import { fetchFlatFileSchema } from "../services/flatFileService";

const FlatFileConfig = ({ selectionType, formDataRef, setColumns }) => {
  const [inputMode, setInputMode] = useState("path"); // "path" or "file"

  const [fileConfig, setFileConfig] = useState({
    fileName: "", // URL path or name from file
    delimiter: ",",
    hasHeader: true,
    encoding: "UTF-8",
  });

  // Handles input field changes (text fields)
  const handleInputChange = (e) => {
    let { id, value } = e.target;

    if (id === "fileName") {
      // If the user is manually typing a file path, remove the uploaded file
      value = value.replace(/^"(.*)"$/, "$1");
      formDataRef.current.delete("file");
    }

    if (["delimiter", "encoding", "fileName"].includes(id)) {
      setFileConfig((prev) => ({
        ...prev,
        [id]: value,
      }));
    }
  };

  // Handles checkbox state (hasHeader)
  const handleCheckboxChange = (e) => {
    setFileConfig((prev) => ({
      ...prev,
      hasHeader: e.target.checked,
    }));
  };

  // Handles file upload and updates fileName in config
  const handleFileUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      // Store the uploaded file
      formDataRef.current.set("file", file);

      // Clear any previously set fileName (for path mode)
      setFileConfig((prev) => ({
        ...prev,
        fileName: "", // Clear the file path when a file is uploaded
      }));
    }
  };

  const fetchSchema = () => {
    fetchFlatFileSchema(formDataRef)
      .then((res) => setColumns(res.columns))
      .catch((err) => console.error(err));
  };

  // Keep formDataRef updated whenever fileConfig changes
  useEffect(() => {
    formDataRef.current.set("flatFileConfig", JSON.stringify(fileConfig));
  }, [fileConfig]);

  return (
    <div
      id="flatfileSourceConfig"
      className="mb-4 border border-gray-400 rounded shadow"
    >
      <div className="bg-blue-600 text-white px-4 py-2 rounded-t">
        <h5 className="mb-0 text-lg font-semibold">
          Step 2: Configure Flat File{" "}
          {selectionType.charAt(0).toUpperCase() + selectionType.slice(1)}
        </h5>
      </div>

      <div className="p-4 bg-white rounded-b">
        {/* Input mode selection */}
        <div className="mb-4">
          <label className="block mb-2 text-md font-mono text-gray-700">
            Choose Input Type:
          </label>
          <div className="flex space-x-4">
            <label className="flex items-center space-x-2">
              <input
                type="radio"
                name="inputMode"
                value="path"
                checked={inputMode === "path"}
                onChange={() => setInputMode("path")}
                className="w-4 h-4 text-blue-600 border-gray-400"
              />
              <span className="text-gray-700">File Path (URL)</span>
            </label>
            <label className="flex items-center space-x-2">
              <input
                type="radio"
                name="inputMode"
                value="file"
                checked={inputMode === "file"}
                onChange={() => setInputMode("file")}
                className="w-4 h-4 text-blue-600 border-gray-400"
              />
              <span className="text-gray-700">Upload File</span>
            </label>
          </div>
        </div>

        {/* File path or file upload */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              {inputMode === "path" ? "File Path:" : "Upload File:"}
            </label>
            {inputMode === "path" ? (
              <input
                type="text"
                id="fileName"
                value={fileConfig.fileName}
                onChange={handleInputChange}
                placeholder="/path/to/file.csv"
                className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
              />
            ) : (
              <input
                type="file"
                id="fileUpload"
                accept=".csv,.txt"
                onChange={handleFileUpload}
                className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
              />
            )}
          </div>

          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              Delimiter:
            </label>
            <input
              type="text"
              id="delimiter"
              value={fileConfig.delimiter}
              onChange={handleInputChange}
              placeholder=","
              className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="hasHeader"
              checked={fileConfig.hasHeader}
              onChange={handleCheckboxChange}
              className="w-4 h-4 text-blue-600 border-gray-400 focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
            <label htmlFor="hasHeader" className="text-md text-gray-700">
              File has header row
            </label>
          </div>

          <div>
            <label className="block mb-1 text-md font-mono text-gray-700">
              Encoding:
            </label>
            <input
              type="text"
              id="encoding"
              value={fileConfig.encoding}
              onChange={handleInputChange}
              placeholder="UTF-8"
              className="w-full border border-gray-400 px-3 py-2 rounded focus:outline-none focus:ring-3 focus:ring-[rgba(70,130,246,0.6)]"
            />
          </div>
        </div>

        <div>
          <button
            id="loadFlatfileColumns"
            onClick={fetchSchema}
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
