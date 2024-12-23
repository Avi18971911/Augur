import {NavLink} from "react-router-dom";
import routes from "../config/routes.ts";
import styles from "../styles/NavLink.module.css";

function Toolbar() {
    return (
        <nav style={{ display: 'flex', width: '100%', position: 'fixed', top: 0, left: 0, background: 'gray' }}>
            <ul style={{ display: 'flex', listStyle: 'none', gap: '1rem', padding: 0, marginLeft: 20 }}>
                {Object.entries(routes).map(([key, route]) => (
                    <li key={key}>
                        <NavLink
                            to={route.path}
                            className={({ isActive }) =>
                                isActive ? styles.active : styles.inactive
                            }
                        >
                            {route.label}
                        </NavLink>
                    </li>
                ))}
            </ul>
        </nav>
    );
}

export default Toolbar