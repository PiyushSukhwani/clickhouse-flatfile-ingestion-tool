import React from "react";
import ClickHouseConfig from "./ClickHouseConfig";
import FlatFileConfig from "./FlatFileConfig";

const ConfigRenderer = ({
  type,
  selectionType = "source",
  setColumns = () => {},
  formDataRef,
}) => {
  if (type === "clickhouse") {
    return (
      <ClickHouseConfig
        formDataRef={formDataRef}
        selectionType={selectionType}
        {...(selectionType === "source" && { setColumns })}
      />
    );
  }

  if (type === "flatfile") {
    return (
      <FlatFileConfig
        formDataRef={formDataRef}
        selectionType={selectionType}
        {...(selectionType === "source" && { setColumns })}
      />
    );
  }

  return null;
};

// Export memoized version
export default React.memo(ConfigRenderer, (prevProps, nextProps) => {
  return (
    prevProps.type === nextProps.type &&
    prevProps.selectionType === nextProps.selectionType
  );
});
