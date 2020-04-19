import React, { Component } from "react";
import BootstrapTable from "react-bootstrap-table-next";
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css";
class PreviewDatabase extends Component {
  render() {
    if (
      this.props.databasePreview === undefined ||
      this.props.databasePreview === null
    ) {
      return (
        <div>
          <p className="lead text-center mt-5 pt-5">
            Select schema to preview data
          </p>
        </div>
      );
    } else {
      return (
        <div>
          {this.props.databasePreview.map((tableDetail, index) => {
            return (
              <div key={tableDetail.name}>
                <div className="row">
                  <div className="col-md-12">
                    <h5 key={tableDetail.name} className="p-0 mb-1 mt-4">
                      {tableDetail.name}
                    </h5>
                    <BootstrapTable
                      striped
                      bordered={false}
                      hover
                      condensed
                      bootstrap4={true}
                      headerClasses="bg-secondary text-white"
                      keyField={tableDetail.name + "-data"}
                      key={tableDetail.name + "-data"}
                      data={tableDetail.data}
                      columns={tableDetail.columns}
                    />
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      );
    }
  }
}

export default PreviewDatabase;
