import React, { Component } from "react";
import "./index.css";
class DBConnectedHeader extends Component {
  state = {};
  constructor(props) {
    super(props);
    this.state = {
      schemaName: ""
    };
    const schemaName = localStorage.getItem("schemaName");
    if (schemaName != null) {
      this.props.fetchDatabasePreview(schemaName);
      this.props.fetchCAvSATConstraints(schemaName);
    }
  }

  componentDidMount() {
    const schemaName = localStorage.getItem("schemaName");
    if (schemaName === null || schemaName === "") return;
    const dropDown = document.getElementById("selectSchema");
    for (var i = 0; i < dropDown.options.length; i++) {
      if (dropDown.options[i].value === schemaName) {
        dropDown.options[i].selected = true;
        break;
      }
    }
  }

  disconnectDB(event) {
    this.props.handleDBConnected(false, null);
  }

  handleInputChange(event) {
    event.preventDefault();
    this.setState({
      schemaName: event.target.value
    });
    localStorage.setItem("schemaName", event.target.value);
    this.props.fetchDatabasePreview(event.target.value);
    this.props.fetchCAvSATConstraints(event.target.value);
  }

  render() {
    return (
      <form className="form-inline my-2 my-lg-0">
        <p className="my-0 mr-sm-2 text-white">
          {this.props.dbEnv.dbDisplayName} at {this.props.dbEnv.serverIPAddress}
        </p>
        <div className="input-group mr-sm-2">
          <select
            className="form-control form-control-sm"
            id="selectSchema"
            name="selectSchema"
            onChange={this.handleInputChange.bind(this)}
          >
            <option value="">Select Schema</option>
            {this.props.dbEnv.schemas.map(schema => (
              <option key={schema} value={schema}>
                {schema}
              </option>
            ))}
          </select>
          <div className="input-group-append">
            <button
              className="btn btn-secondary btn-sm form-control form-control-sm"
              type="button"
              data-toggle="modal"
              data-target="#constraintsModal"
            >
              Constraints
            </button>
          </div>
        </div>

        <button
          className="btn btn-danger btn-sm form-control form-control-sm"
          type="button"
          onClick={this.disconnectDB.bind(this)}
        >
          Disconnect
        </button>
      </form>
    );
  }
}

export default DBConnectedHeader;
