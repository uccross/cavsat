import React, { Component } from "react";
import { ForceGraph3D } from "react-force-graph";
import SpriteText from "three-spritetext";
import sqlFormatter from "sql-formatter";
import Modal from "./Modal";

class DataComplexity extends Component {
  render() {
    const queryAnalysis = this.props.queryAnalysis;
    function ConquerRewriting() {
      if (queryAnalysis.conquerRewriting === undefined) return "";
      return (
        <div>
          <a
            href="#"
            className="lead m-0 p-0"
            data-toggle="modal"
            data-target={"#modal-conquer"}
          >
            {" "}
            View ConQuer SQL-rewriting
          </a>
          <Modal
            modalId="modal-conquer"
            modalHeader="ConQuer SQL Rewriting"
            tabHeaders={["SQL"]}
            tabBodies={[sqlFormatter.format(queryAnalysis.conquerRewriting)]}
          />
        </div>
      );
    }

    function KWRewriting() {
      if (queryAnalysis.kwRewriting === undefined) return "";
      return (
        <div>
          <a
            href="#"
            className="lead m-0 p-0"
            data-toggle="modal"
            data-target={"#modal-kw"}
          >
            {" "}
            View Koutris-Wijsen SQL-rewriting
          </a>
          <Modal
            modalId="modal-kw"
            modalHeader="Koutris-Wijsen SQL Rewriting"
            tabHeaders={["SQL", "Tuple Relational Calculus"]}
            tabBodies={[
              sqlFormatter.format(queryAnalysis.kwRewriting.sql),
              queryAnalysis.kwRewriting.trc
            ]}
          />
        </div>
      );
    }

    if (queryAnalysis === undefined) {
      return (
        <div>
          <p className="lead text-center mt-5 pt-5">
            Execute query to see analysis
          </p>
        </div>
      );
    } else {
      return (
        <div>
          <div className="row border-between">
            <div className="col-md-2 mt-0 pt-4">
              <h5 className="p-0 mb-1">Data Complexity</h5>
            </div>
            <div className="col-md-5 mt-0 pt-4 text-center">
              <h5 className="p-0 mb-1">Attack Graph</h5>
            </div>
            <div className="col-md-5 mt-0 pt-4">
              <h5 className="p-0 mb-1 text-center">Join Graph</h5>
            </div>
          </div>
          <div className="row border-between">
            <div className="col-md-2 mt-0 pt-4">
              <p className="lead m-0 p-0">
                {queryAnalysis.dataComplexityDescription}{" "}
              </p>
              <p className="lead m-0 p-0">
                (Analysis completed in {queryAnalysis.complexityAnalysisTime}{" "}
                ms)
              </p>
              <br />
              <ConquerRewriting />
              <KWRewriting />
            </div>
            <div className="col-md-5 mt-0 pt-4">
              <ForceGraph3D
                graphData={queryAnalysis.attackGraph}
                linkAutoColorBy="value"
                backgroundColor="white"
                width={(window.innerWidth * 5) / 12}
                height={window.innerHeight / 2}
                showNavInfo={false}
                nodeOpacity={1}
                linkOpacity={1}
                linkDirectionalArrowLength={3}
                linkDirectionalArrowRelPos={1}
                linkCurvature={0.25}
                nodeThreeObject={node => {
                  const sprite = new SpriteText(node.id);
                  sprite.color = "rgba(5, 75, 133, 1)";
                  sprite.textHeight = 5;
                  return sprite;
                }}
              />
            </div>
            <div className="col-md-5 mt-0 pt-4">
              <ForceGraph3D
                graphData={queryAnalysis.joinGraph}
                backgroundColor="white"
                width={(window.innerWidth * 5) / 12}
                height={window.innerHeight / 2}
                showNavInfo={false}
                nodeOpacity={1}
                linkOpacity={1}
                linkDirectionalArrowLength={3}
                linkDirectionalArrowRelPos={1}
                linkCurvature={0.25}
                nodeThreeObject={node => {
                  const sprite = new SpriteText(node.id);
                  sprite.color = "rgba(133, 5, 5, 1)";
                  sprite.textHeight = 5;
                  return sprite;
                }}
              />
            </div>
          </div>
        </div>
      );
    }
  }
}
export default DataComplexity;
