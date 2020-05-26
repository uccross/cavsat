/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.util;

import edu.cavsat.model.bean.DBEnvironment;

/**
 * @author Akhil
 *
 */
public class DBUtil {
	public static String constructConnectionURL(DBEnvironment dbEnv, String schemaName) {
		switch (dbEnv.getDbEngine().toLowerCase()) {
		case "sqlserver":
			String url = String.format("jdbc:sqlserver://%s:%s;database=%s;user=%s@cqa;password=%s;",
					dbEnv.getServerIPAddress(), dbEnv.getServerPort(), schemaName, dbEnv.getUsername(),
					dbEnv.getPassword());
			return url;
		// encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
		default:
			return null;
		}
	}
}
