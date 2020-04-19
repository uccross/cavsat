import React, { Component } from "react";

class ConfigForm extends Component {
  handleConfigInputChange(event) {
    const value = event.target.value;
    const name = event.target.name;
    this.setState({
      [name]: value
    });
    if (name === "dbEngine") {
      const dbDisplayName = event.target.options[
        event.target.selectedIndex
      ].getAttribute("data-displayname");
      this.setState({
        dbDisplayName: dbDisplayName
      });
    }
  }

  async handleSubmit(event) {
    event.preventDefault();
    console.log("state" + this.state);
    const response = await fetch("api/check-jdbc-connection", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json"
      },
      body: JSON.stringify(this.state)
    });
    const responseBody = await response.json();
    if (response.status === 200) {
      this.props.handleDBConnected(true, responseBody);
    }
  }

  render() {
    return (
      <main role="main" className="container">
        <form onSubmit={this.handleSubmit.bind(this)}>
          <div className="row form-group">
            <div className="col-md-4 mx-md-0">
              <select
                className="form-control"
                name="dbEngine"
                onChange={this.handleConfigInputChange.bind(this)}
              >
                <option>Select DB Engine</option>
                <option value="sqlserver" data-displayname="MS SQL Server">
                  MS SQL Server
                </option>
                <option value="postgresql" data-displayname="PostgreSQL">
                  PostgreSQL
                </option>
                <option value="mysql" data-displayname="MySQL">
                  MySQL
                </option>
              </select>
            </div>
          </div>
          <div className="row form-group">
            <div className="col-md-3 mx-md-0">
              <input
                type="text"
                name="serverIPAddress"
                className="form-control"
                id="serverIPAddress"
                placeholder="Server IP Address"
                onChange={this.handleConfigInputChange.bind(this)}
              />
            </div>
            <div className="col-md-1 pl-md-0 mx-md-0">
              <input
                type="text"
                name="serverPort"
                className="form-control w80"
                id="serverPort"
                placeholder="Port"
                onChange={this.handleConfigInputChange.bind(this)}
              />
            </div>
          </div>
          <div className="row form-group">
            <div className="col-md-2 mx-md-0">
              <input
                type="text"
                name="username"
                className="form-control"
                id="username"
                placeholder="Username"
                onChange={this.handleConfigInputChange.bind(this)}
              />
            </div>
            <div className="col-md-2 pl-md-0 mx-md-0">
              <input
                type="password"
                name="password"
                className="form-control"
                id="password"
                placeholder="Password"
                onChange={this.handleConfigInputChange.bind(this)}
              />
            </div>
          </div>
          <div className="row form-group">
            <div className="col-md-4 mx-md-0">
              <button type="submit" className="btn btn-block btn-primary">
                Connect
              </button>
            </div>
          </div>
        </form>
      </main>
    );
  }
}
export default ConfigForm;
