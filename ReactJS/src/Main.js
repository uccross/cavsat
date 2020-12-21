import React, { Component } from "react";
import ConfigForm from "./config-form";
import QueryForm from "./query-form";
import TopNavbar from "./TopNavbar";
import FrontpageDescription from "./FrontpageDescription";
import ConstraintsModal from "./ConstraintsModal";
import "bootstrap/dist/css/sticky-footer-navbar.css";

class Main extends Component {
  constructor(props) {
    super(props);
    this.state = {
      connectedToDB: false,
      dbEnv: null,
      databasePreview: null,
      constraints: [],
      schemaName: localStorage.getItem("schemaName"),
    };
  }

  componentWillMount() {
    const dbEnv = JSON.parse(localStorage.getItem("dbEnv"));
    if (dbEnv === null) {
      this.handleDBConnected(false, null);
    } else {
      this.handleDBConnected(true, dbEnv);
    }
    console.log("willmount");
  }

  handleDBConnected(value, responseBody) {
    if (value === true) {
      localStorage.setItem("dbEnv", JSON.stringify(responseBody));
      this.setState({
        connectedToDB: true,
        dbEnv: responseBody,
      });
    } else {
      localStorage.removeItem("dbEnv");
      localStorage.removeItem("schemaName");
      this.setState({
        connectedToDB: false,
        dbEnv: null,
        databasePreview: null,
        constraints: null,
      });
    }
  }

  async fetchDatabasePreview(schemaName) {
    if (schemaName === null || schemaName === undefined || schemaName === "") {
      this.setState({ databasePreview: null });
      return;
    }
    const response = await fetch("api/get-database-preview", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.state.dbEnv,
        schemaName: schemaName,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      databasePreview: responseBody,
    });
    console.log(responseBody);
  }

  async fetchCAvSATConstraints(schemaName) {
    if (schemaName === null || schemaName === undefined || schemaName === "") {
      this.setState({ constraints: [] });
      return;
    }
    const response = await fetch("api/get-cavsat-constraints", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.state.dbEnv,
        schemaName: schemaName,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      constraints: responseBody,
    });
  }

  render() {
    if (!this.state.connectedToDB) {
      return (
        <div>
          <TopNavbar
            handleDBConnected={this.handleDBConnected.bind(this)}
            fetchDatabasePreview={this.fetchDatabasePreview.bind(this)}
            fetchCAvSATConstraints={this.fetchCAvSATConstraints.bind(this)}
            dbEnv={this.state.dbEnv}
          />
          <FrontpageDescription />
          <ConfigForm handleDBConnected={this.handleDBConnected.bind(this)} />
        </div>
      );
    } else {
      return (
        <div>
          <TopNavbar
            handleDBConnected={this.handleDBConnected.bind(this)}
            fetchDatabasePreview={this.fetchDatabasePreview.bind(this)}
            fetchCAvSATConstraints={this.fetchCAvSATConstraints.bind(this)}
            dbEnv={this.state.dbEnv}
            constraints={this.state.constraints}
          />
          <ConstraintsModal
            schemaName={this.state.schemaName}
            constraints={this.state.constraints}
          />
          <QueryForm
            dbEnv={this.state.dbEnv}
            databasePreview={this.state.databasePreview}
          />
        </div>
      );
    }
  }
}

export default Main;
